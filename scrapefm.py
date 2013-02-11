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
import types

API_KEY_VAR = "LAST_FM_API_PKEY"

LOGGER = logging.getLogger('scrapefm')
DBASE  = peewee.SqliteDatabase(None)

class BaseModel(peewee.Model):
    """ All models inherit base. """
    class Meta:
        """ Link all models to database. """
        database = DBASE

class Users(BaseModel):
    """ Last.fm user table. """
    id          = peewee.IntegerField(primary_key=True)
    name        = peewee.CharField()
    age         = peewee.IntegerField(null=True)
    country     = peewee.CharField(null=True)
    gender      = peewee.CharField(null=True)
    playcount   = peewee.IntegerField(default=0)
    subscriber  = peewee.BooleanField(default=False)

class Artists(BaseModel):
    """ Last.fm artist table. """
    name        = peewee.TextField()
    playcount   = peewee.IntegerField(default=0)
    tagcount    = peewee.IntegerField(default=0)

class Tags(BaseModel):
    """ Tags table. """
    name        = peewee.CharField()

class Friends(BaseModel):
    """ Friendship edges. """
    a = peewee.ForeignKeyField(Users)
    b = peewee.ForeignKeyField(Users)

class WeeklyArtistChart(BaseModel):
    """ Friendship edges. """
    weekfrom    = peewee.CharField()
    user        = peewee.ForeignKeyField(Users)
    artist      = peewee.ForeignKeyField(Artists)
    playcount   = peewee.IntegerField()

class ArtistTags(BaseModel):
    """ Tags associated to artist. """
    artist      = peewee.ForeignKeyField(Artists)
    tag         = peewee.ForeignKeyField(Tags)
    count       = peewee.IntegerField()

class ScraperException(Exception):
    """ Raised if too many internal errors from Last.fm API """


class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    ERRLIM = 10     # errors before quitting
    COMLIM = 1      # outstanding transactions before commit

    def __init__(self, options):
        self.__dict__ = dict(options.__dict__.iteritems())
        self.network = pylast.LastFMNetwork(api_key = self.api_key)
        self.network.enable_caching()
        self.errcnt = 0
        self.commit = 0

        self.load_db()

    def load_db(self):
        """ Create tables if necessary """
        DBASE.init(self.db)
        DBASE.set_autocommit(False)
        DBASE.connect()
        Users.create_table(fail_silently=True)
        Artists.create_table(fail_silently=True)
        Tags.create_table(fail_silently=True)
        Friends.create_table(fail_silently=True)
        WeeklyArtistChart.create_table(fail_silently=True)
        ArtistTags.create_table(fail_silently=True)

        try:
            Artists.select().where(Artists.name == '').get()
        except Artists.DoesNotExist:
            # dud artist to populate chart row in absence of scrobbles
            Artists.create(name = '') 

        self.db = DBASE
        # name -> id mapping for all scraped users and artists.
        self.users   = dict([ (u.name, u.id) for u in Users.select() ])
        self.artists = dict([ (a.name, a.id) for a in Artists.select() ])
        return 

    def _get_friends(self, user):
        return [ friend.name for friend in user.get_friends(self.maxfriends) ]
    
    def _get_weeks(self):
        """ Get weeks whose starting date matches provided pattern. """
        user    = self.network.get_user('RJ')
        pattern = re.compile(self.datematch)
        unix_to_date = lambda x: datetime.fromtimestamp(x).strftime(self.datefmt)

        weeks = []
        for weekfrom, weekto in user.get_weekly_chart_dates():
            if pattern.match( unix_to_date(int(weekfrom)) ):
                weeks.append( (weekfrom, weekto) )
        return weeks

    def rescrape(self, weeks):
        """ Load information from database. """
        # get all possible weeks matching desired pattern.
        toscrape = set([ weekfrom for weekfrom, _ in weeks ])
        fromto   = dict(weeks)

        select        = WeeklyArtistChart.select(WeeklyArtistChart.weekfrom)
        weeks_by_user = lambda x: select.where(WeeklyArtistChart.user == x).distinct()

        for username, userid in self.users.iteritems():
            done    = set([w.weekfrom for w in weeks_by_user(userid)])
            notdone = toscrape.difference(done)
            if len(notdone):
                LOGGER.debug("Rescraping %d: %s", userid, username)
                self.scrape_user(username, [(w, fromto[w]) for w in notdone ])

    def scrape_artist(self, name):
        LOGGER.info("Found new artist: %s.", name)
        assert not Artists.select().where(Artists.name == name).exists()
        return Artists.create(name = name).id

    def scrape_friends(self, user, userid):
        """ Given user, explore connected nodes.
            If a neighbour node has already been seen, add edge to friend table.
            Otherwise, return unexplored neighbour.
        """
        for friend in self._get_friends(user):
            if friend not in self.users:
                continue
            ordered = dict( zip(('a', 'b'), sorted([ userid, self.users[friend] ])) )
            try:
                Friends.get( **ordered )
            except Friends.DoesNotExist:
                LOGGER.debug("Connecting %s with %s.", *ordered.values())
                Friends.create( **ordered )

    @DBASE.commit_on_success
    def scrape_user(self, username, weeks):
        """ Scrape user info and return ID. """
        LOGGER.info("Adding user %s.", username)
        user   = self.network.get_user(username)
        
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

    def create_user(self, user):
        """ Retrieves user information from last.fm and inserts to database.
        """
        # hardwire function so that fields in Users table map
        # to get_* function in pylast.Users class
        user.get_subscriber = user.is_subscriber
        values = {}
        for field in Users._meta.get_fields():
            values[field.name] = field.db_value(getattr(user, 'get_%s' % field.name)())
        return Users.create( **values ).id

    def scrape_week(self, user, userid, weekfrom, weekto):
        """ Scrape single week, inserting artists if not cached.  """
        LOGGER.debug("Scraping week %s for user %s.", weekfrom, user)

        row = { 'weekfrom': weekfrom, 'user': userid }

        for artist, count in user.get_weekly_artist_charts(weekfrom, weekto):
            if artist.name not in self.artists:
                self.artists[artist.name] = self.scrape_artist(artist.name)
            row.update({'artist': self.artists[artist.name], 'playcount': count})
            WeeklyArtistChart.create( **row )

        if 'artist' not in row:
            LOGGER.debug("No chart, adding empty entry as placeholder.")
            row.update({'artist': self.artists[''], 'playcount': 0})
            WeeklyArtistChart.create( **row )
               
    def close(self):
        """ Close database, rolling back any uncommitted changes. """
        self.db.commit()
        self.db.close()

    def run(self):
        """ Start from seed user and scrape info by following social graph.
        """
        weeks = self._get_weeks()

        # verify partial entries are completed first before adding new
        self.rescrape(weeks)

        scraped = self.users
        queue   = []

        # auxiliary function to sample at most n items from x
        nsample = lambda x, n: random.sample(x, min(len(x), n))

        # use seed if starting from scratch
        if not len(scraped):
            queue.append( self.username )

        while len(scraped) < self.limit:
            while not len(queue):
                # sample neighbours from random scraped user.
                user  = self.network.get_user( random.choice(scraped.keys()) )
                queue = nsample( self._get_friends(user), 10 )
            username = queue.pop()
            if username not in scraped:
                scraped[username] = self.scrape_user(username, weeks)


def parse_args():
    """ Parse command line options.
    """
    parser = argparse.ArgumentParser(description=
            'A Last.fm scraper.'
            )

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
    """ Main entry point
    """
    options = parse_args()
    options.maxfriends = 1000
    options.do_connect = False
    options.datefmt    = "%Y-%m"
    options.datematch  = "2013-0?"

    scraper = Scraper(options)
    try:
        scraper.run()
        #scraper.populate_friends()
        #scraper.populate_charts('%Y-%m','2013-01')
    except (ScraperException, KeyboardInterrupt):
        scraper.db.rollback()
    scraper.close()

if __name__ == "__main__":
    main()
