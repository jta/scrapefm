#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Author: João Taveira Araújo (first second at gmail / @jta)
    scrapefm.py: Last.fm scraper
"""
import argparse
from datetime import datetime
import logging
import peewee
import pylast
import os
import random
import re
import urlparse

# environment variables
API_KEY_VAR = "LAST_FM_API_PKEY"
HTTP_PROXY = "HTTP_PROXY"

LOGGER = logging.getLogger('scrapefm')
DBASE = peewee.SqliteDatabase(None)


class BaseModel(peewee.Model):
    """ All models inherit base. """
    class Meta:
        """ Link all models to database. """
        database = DBASE


class Users(BaseModel):
    """ Last.fm user table. """
    id = peewee.IntegerField(primary_key=True)
    name = peewee.CharField()
    age = peewee.IntegerField(null=True)
    country = peewee.CharField(null=True)
    gender = peewee.CharField(null=True)
    playcount = peewee.IntegerField(default=0)
    subscriber = peewee.BooleanField(default=False)


class Artists(BaseModel):
    """ Last.fm artist table. """
    name = peewee.TextField()
    playcount = peewee.IntegerField(default=0)
    tagcount = peewee.IntegerField(default=0)


class Tags(BaseModel):
    """ Tags table. """
    name = peewee.CharField()


class Friends(BaseModel):
    """ Friendship edges. """
    a = peewee.ForeignKeyField(Users)
    b = peewee.ForeignKeyField(Users)


class WeeklyArtistChart(BaseModel):
    """ Friendship edges. """
    user = peewee.ForeignKeyField(Users)
    artist = peewee.ForeignKeyField(Artists)
    weekfrom = peewee.CharField()
    playcount = peewee.IntegerField()


class ArtistTags(BaseModel):
    """ Tags associated to artist. """
    artist = peewee.ForeignKeyField(Artists)
    tag = peewee.ForeignKeyField(Tags)


class ScraperException(Exception):
    """ Raised if too many internal errors from Last.fm API """


class Scraper(object):
    """ Use Last.fm API to retrieve data to local database.
    """
    ERRLIM = 100     # errors before quitting

    def __init__(self, options):
        """ Import attributes from options and load data.

        Attributes
        ----------
        network : pylast.LastFMNetowrk
            Wrapper for Last.FM API.
        errcnt : int
            Counter for pylast.* exceptions observed.
        users : dict
            Scraped users. Maps username to user ID.
        artists : dict
            Scraped artists, mapping artist name to artist key.
        tags : dict
            Scraped tags, mapping tag name to tag key.
        """
        self.__dict__ = dict(options.__dict__.iteritems())
        self.network = pylast.LastFMNetwork(api_key=self.api_key)
        self.errcnt = 0

        self.initdb()
        self.tags = dict([(t.name, t.id) for t in Tags.select()])
        self.users = dict([(u.name, u.id) for u in Users.select()])
        self.artists = dict([(a.name, a.id) for a in Artists.select()])

        self.network.enable_caching()

    @classmethod
    def close(cls):
        """ Close database, wrap-up. """
        DBASE.close()

    @classmethod
    def create_tag(cls, tag):
        """ Insert tag to database and return key

            Parameters
            ----------
            tag : str
                Tag description
        """
        LOGGER.info("Found new tag: %s.", tag)
        return Tags.create(name=tag).id

    @classmethod
    def create_user(cls, user):
        """ Retrieves user information from last.fm and inserts to database.

            Parameters
            ----------
            user : pylast.User
                User to be retrieved.
        """
        LOGGER.info("Adding user %s.", user)
        doc = user._request('user.getInfo', True)

        fields = Users._meta.get_fields()
        getvar = lambda x: field.db_value(pylast._extract(doc, x.name))
        values = dict([(field.name, getvar(field)) for field in fields])
        return Users.create(**values).id

    def _get_friends(self, user):
        """ Returns list of friends.

            Parameters
            ----------
            user : pylast.User
                User object to retrieve friends for.
        """
        return [friend.name for friend in user.get_friends(self.maxfriends)]

    def _get_weeks(self, datefmt, datematch):
        """ Returns list of weeks in (weekfrom, weekto) form
            matching given date pattern.

            Parameters
            ----------
            datefmt : str
                A valid format code for strftime()
            datematch : str
                A regexp on which to match charted weeks after
                converting UNIX timestamp to provided datefmt.
        """
        pattern = re.compile(datematch)
        unix_to_date = lambda x: datetime.fromtimestamp(x).strftime(datefmt)
        is_matched = lambda (start, _): pattern.match(unix_to_date(int(start)))
        all_weeks = self.network.get_user('RJ').get_weekly_chart_dates()

        return [week for week in all_weeks if is_matched(week)]

    def handle_api_errors(func):
        """ Handler for pylast Exceptions. Wraps commit decorator. """
        def handler(self, *args):
            try:
                func(self, *args)
            except (pylast.WSError,
                    pylast.MalformedResponseError,
                    pylast.NetworkError) as e:
                self.errcnt += 1
                if self.errcnt < self.ERRLIM:
                    LOGGER.error("Error %d: %s" % (self.errcnt, e))
                else:
                    raise ScraperException
        return handler

    def initdb(self):
        """ Load database, creating tables if new.

            When creating Artists table, insert dud artist '' to serve as
            placeholder in absence of scrobbles.
        """
        DBASE.init(self.db)
        DBASE.set_autocommit(False)
        DBASE.connect()

        if not Users.table_exists():
            Users.create_table()
            Artists.create_table()
            Tags.create_table()
            Friends.create_table()
            WeeklyArtistChart.create_table()
            ArtistTags.create_table()
            Artists.create(name='')

        return

    def rescrape(self, weeks):
        """ Iterate over previously scraped users and retrieve missing data.

            Parameters
            ----------
            weeks : list of (str, str)
                List of weeks to scrape.
        """
        # get all possible weeks matching desired pattern.
        toscrape = set([start for start, _ in weeks])
        week_end = dict(weeks)

        select_week = WeeklyArtistChart.select(WeeklyArtistChart.weekfrom)
        filter_user = lambda q, u: q.where(WeeklyArtistChart.user == u).distinct()

        for username, userid in self.users.iteritems():
            done = set(filter_user(select_week, userid))
            print done
            done = set([w.weekfrom for w in filter_user(select_week, userid)])
            print done
            notdone = toscrape.difference(done)
            if len(notdone):
                LOGGER.debug("Rescraping %s", username)
                self.scrape_user(username, [(s, week_end[w]) for s in notdone])

    def run(self):
        """ Start from seed user and scrape info by following social graph.
        """
        weeks = self._get_weeks(self.datefmt, self.datematch)

        # verify partial entries are completed first before adding new
        self.rescrape(weeks)

        scraped = self.users
        queue = []

        # auxiliary function to sample at most n items from x
        nsample = lambda x, n: random.sample(x, min(len(x), n))

        # use seed if starting from scratch
        if not len(scraped):
            queue.append(self.username)

        while len(scraped) < self.limit:
            while not len(queue):
                # sample neighbours from random scraped user.
                user = self.network.get_user(random.choice(scraped.keys()))
                queue = nsample(self._get_friends(user), 10)
            username = queue.pop()
            if username not in scraped:
                scraped[username] = self.scrape_user(username, weeks)

    def scrape_artist(self, name):
        """ Insert artist to database and return key, scraping tags in process.

            Parameters
            ----------
            name : str
                Artist name
        """
        artist = self.network.get_artist(name)
        LOGGER.info("Found new artist: %s.", name)
        if artist.get_name(True) in self.artists:
            return self.artists[artist.get_name(True)]

        doc = artist._request('artist.getInfo', True)
        values = {}
        for field in Artists._meta.get_fields():
            values[field.name] = field.db_value(pylast._extract(doc, field.name))
        values['tagcount'] = 0
        artistid = Artists.create(**values).id
        return artistid

        tags = [pylast._extract(node, "name") for node in doc.getElementsByTagName("tag")]
        for tag in tags:
            if tag not in self.tags:
                self.tags[tag] = self.create_tag(tag)
            ArtistTags.create(artist=artistid, tag=self.tags[tag])

        return artistid

    def scrape_friends(self, user, userid):
        """ Connect user to already scraped friends.

            Parameters
            ----------
            user : pylast.User
                User from which to search for friends.
            userid : int
                Respective user ID
        """
        for friend in self._get_friends(user):
            if friend not in self.users:
                continue
            ordered = dict(zip(('a', 'b'), sorted([userid, self.users[friend]])))
            try:
                Friends.get(**ordered)
            except Friends.DoesNotExist:
                LOGGER.debug("Connecting %s with %s.", *ordered.values())
                Friends.create(**ordered)

    @handle_api_errors
    @DBASE.commit_on_success
    def scrape_user(self, username, weeks):
        """ Scrape user data and return ID.

            Parameters
            ----------
            username : str
                User from which to search for friends.
            weeks : list of (str, str)
        """
        user = self.network.get_user(username)

        if username not in self.users:
            userid = self.create_user(user)
        else:
            userid = self.users[username]

        # link neighbours
        if self.do_connect:
            self.scrape_friends(user, userid)

        # get weekly chart
        for weekfrom, weekto in weeks:
            self.scrape_week(user, userid, weekfrom, weekto)

        return userid

    def scrape_week(self, user, userid, weekfrom, weekto):
        """ Scrape single week, inserting artists if not cached.

            Parameters
            ----------
            user : pylast.User
                User from which to search for friends.
            userid : int
                Respective user ID
            weekfrom : str
                Week start in UNIX timestamp as defined by Last.fm
            weekto : str
                Week end in UNIX timestamp as defined by Last.fm
        """
        LOGGER.debug("Scraping week %s for user %s.", weekfrom, user)

        row = {'weekfrom': weekfrom, 'user': userid}

        for artist, count in user.get_weekly_artist_charts(weekfrom, weekto):
            if artist.name not in self.artists:
                self.artists[artist.name] = self.scrape_artist(artist.name)
            row.update({'artist': self.artists[artist.name],
                        'playcount': count})
            WeeklyArtistChart.create(**row)

        if 'artist' not in row:
            LOGGER.debug("No chart, adding empty entry as placeholder.")
            row.update({'artist': self.artists[''], 'playcount': 0})
            WeeklyArtistChart.create(**row)


def parse_args():
    """ Parse command line options.
    """
    parser = argparse.ArgumentParser(description='A Last.fm scraper.')

    parser.add_argument('-k', '--key', nargs=1,
                        dest='api_key', action='store', type=str,
                        default=os.environ.get(API_KEY_VAR),
                        help='Last.fm API public key. Alternatively can \
                              be supplied through $%s variable' % API_KEY_VAR)

    parser.add_argument('--seed', dest='username',
                        action='store', type=str, default='RJ',
                        help='Starting point for graph traversal.')

    parser.add_argument('--users', dest='limit',
                        action='store', type=int, default=100,
                        help='Maximum number of users to scrape.')

    group = parser.add_mutually_exclusive_group()

    group.add_argument('--debug', dest='debug',
                       action='store_true', default=False,
                       help='Print debug information.')

    group.add_argument('--quiet', dest='quiet',
                       action='store_true', default=False,
                       help='Output errors only.')

    parser.add_argument("db", type=str,
                        help="Database to be written to.")

    args = parser.parse_args()

    logging.basicConfig()
    if args.debug:
        LOGGER.setLevel(logging.DEBUG)
    elif args.quiet:
        LOGGER.setLevel(logging.ERROR)
    else:
        LOGGER.setLevel(logging.INFO)

    if not args.api_key:
        parser.error('No API key provided in options or $%s' % API_KEY_VAR)

    return args


def main():
    """ Main entry point """
    options = parse_args()
    options.maxfriends = 1000
    options.do_connect = False
    options.datematch = "2013-01"
    options.datefmt = "%Y-%m"

    scraper = Scraper(options)

    if os.environ.get(HTTP_PROXY):
        url = urlparse.urlparse(os.environ.get(HTTP_PROXY))
        self.network.enable_proxy(url.hostname, url.port)

    try:
        scraper.run()
    except (ScraperException, KeyboardInterrupt):
        pass
    scraper.close()

if __name__ == "__main__":
    main()
