#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Author: João Taveira Araújo (first second at gmail / @jta)
    scrapefm.py: Last.fm scraper
"""
import argparse
from datetime import datetime
import lastdb
import logging
import pylast
import os
import random
import types

API_KEY_VAR = "LAST_FM_API_PKEY"

LOGGER = logging.getLogger('scrapefm')

class ScraperException(Exception):
    """ Raised if too many internal errors from Last.fm API """

class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    ERRLIM = 10     # errors before quitting
    COMLIM = 100    # outstanding transactions before commit

    def __init__(self, dbname, api_key):
        self.db = lastdb.load(dbname)
        self.network = pylast.LastFMNetwork(api_key = api_key)
        self.network.enable_caching()
        self.errcnt = 0
        self.commit = 0

    def _commit_or_roll(func):
        """ Commit to db or rollback.
            Handles pylast.* exceptions gracefully. 
        """
        def handle(self, *args):
            retval = None
            try:
                retval = func(self, *args)
                # evaluate generator if necessary
                if isinstance(retval, types.GeneratorType):
                    retval = list(retval)
            except (pylast.NetworkError, 
                    pylast.WSError, 
                    pylast.MalformedResponseError) as e:
                self.errcnt += 1
                LOGGER.error("%s. %d errors so far." % (e, self.errcnt))
                self.db.rollback()
            else:
                self.commit += 1
                if not self.commit % self.COMLIM:
                    LOGGER.info("Commit number %d", self.commit / self.COMLIM)
                    self.db.commit()

            if self.errcnt >= self.ERRLIM:
                raise ScraperException
            return retval
        return handle

    @_commit_or_roll
    def _friend_discovery(self, username, users, maxfriends = 50):
        """ Given user, explore connected nodes.
            If a neighbour node has already been seen, add edge to friend table.
            Otherwise, return unexplored neighbour.
        """
        user = self.network.get_user(username)
        for friend in user.get_friends(maxfriends):
            if friend.name in users:
                ordered = dict( zip(('a', 'b'), 
                                sorted([users[username], users[friend.name]])) )
                try:
                    lastdb.Friends.get( **ordered )
                except lastdb.Friends.DoesNotExist:
                    LOGGER.debug("Connecting %s with %s.", *ordered.values())
                    lastdb.Friends.create( **ordered )
            else:
                yield friend.name

    def _get_weeks(self, datefmt, datematch):
        """ Rather than query week list for each user, nick list from one of
            longest serving users (RJ -> founder) and filter
        """
        weeklist = []
        veteran  = self.network.get_user('RJ')
        for weekfrom, weekto in veteran.get_weekly_chart_dates():
            date = datetime.fromtimestamp(int(weekfrom))
            if date.strftime(datefmt) == datematch:
                weeklist.append( (weekfrom, weekto) )
        return weeklist

    def _scrape_artist(self, name):
        LOGGER.info("Found new artist: %s.", name)
        assert not lastdb.Artists.select()\
                        .where(lastdb.Artists.name == name).exists()
        return lastdb.Artists.create(name = name).id

    @_commit_or_roll
    def _scrape_user(self, username):
        """ Scrapes all info associated to username into database.
            Returns user id.
        """
        user   = self.network.get_user(username)

        # fields in lastdb.Users table maps to respective get function
        # in pylast.Users class, except for subscriber which we hardwire
        user.get_subscriber = user.is_subscriber
        values = {}
        for field in lastdb.Users._meta.get_fields():
            values[field.name] = getattr(user, 'get_%s' % field.name)() 

        # annoyingly get_country returns class rather than string.
        if not values['country'].get_name():
            values['country'] = None

        # filter out fields with no values and let defaults take over.
        values = dict((k, v) for k, v in values.items() if v)
        LOGGER.info("Adding user %s.", user)
        return lastdb.Users.create( **values ).id

    @_commit_or_roll
    def _scrape_week(self, username, userid, weekfrom, weekto, artistcache):
        """ Scrape single week, inserting artists if not cached.  """
        LOGGER.debug("Scraping week %s for user %s.", weekfrom, username)
        user   = self.network.get_user(username)

        row = { 'weekfrom': weekfrom, 'user': userid, 
                'artist': artistcache[''], 'playcount': 0 }

        for artist, count in user.get_weekly_artist_charts(weekfrom, weekto):
            if artist.name not in artistcache:
                artistcache[artist.name] = self._scrape_artist(artist.name)
            row['artist'] = artistcache[artist.name]
            row['playcount'] = count
            lastdb.WeeklyArtistChart.create( **row )

        if not row['playcount']:
            LOGGER.debug("No chart, adding fake entry as placeholder.")
            assert row['artist'] == artistcache['']
            lastdb.WeeklyArtistChart.create( **row )
               
    def close(self):
        """ Close database, rolling back any uncommitted changes. """
        self.db.rollback()
        self.db.close()

    def populate_charts(self, datefmt, datematch):
        """ From user list in db, import weekly charts for matching dates.
            Will keep try to re-import weeks which do not exist, i.e. because
            users did not scrobble.
        """
        artists  = dict([ (a.name, a.id) for a in lastdb.Artists.select() ])
        weeklist = self._get_weeks(datefmt, datematch)
        
        for row in lastdb.Users.select():
            username, userid = row.name, row.id
            weeksdone = set()
            try:
                rows = lastdb.WeeklyArtistChart.select()\
                           .where(lastdb.WeeklyArtistChart.user == userid)
                weeksdone.update([row.weekfrom for row in rows])
            except lastdb.WeeklyArtistChart.DoesNotExist:
                pass

            for weekfrom, weekto in weeklist:
                if weekfrom not in weeksdone:
                    self._scrape_week(username, userid, weekfrom, weekto, artists)


    def populate_friends(self):
        visited   = dict([ (u.name, u.id) for u in lastdb.Users.select() ])
        queue     = visited.keys()
        unvisited = set()
        while queue:
            username = queue.pop()
            for new in self._friend_discovery(username, visited, 10000):
                unvisited.add(new)
            LOGGER.debug("%s friends mapped. %d users not covered.", username, len(unvisited))


    def populate_tags(self):
        """ Insert all tags for all artists """
        pass

    def populate_users(self, seed, maxlimit = 100000):
        """ Start from seed user and scrape info by following social graph.
        """
        # set of inspected usernames
        visited   = dict([ (u.name, u.id) for u in lastdb.Users.select() ])
        # set of users (obj) yet to be visited
        username  = seed
        incoming  = []

        while len(visited) < maxlimit:
            while username in visited:
                if not len(incoming):
                    username = random.choice(visited.keys())
                    break
                username = incoming.pop()

            if username not in visited:
                visited[username] = self._scrape_user(username)

            if not len(incoming):
                for new in self._friend_discovery(username, visited, 1000):
                    incoming.append(new)
                # get ten random neighbours
                random.shuffle(incoming)
                incoming = incoming[:10]


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

    parser.add_argument('--seed', dest='seed',
                        action='store', type=str, default='RJ',
                        help='Username for starting point to graph scraping.')

    group = parser.add_mutually_exclusive_group()

    group.add_argument('--debug', dest='debug',
                        action='store_true', default=False,
                        help='Print debug information')

    group.add_argument('--quiet', dest='quiet',
                        action='store_true', default=False,
                        help='Print debug information')

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
    args    = parse_args()
    scraper = Scraper(args.db, args.api_key)

    try:
        scraper.populate_users(args.seed, 100000)
        #scraper.populate_friends()
        scraper.populate_charts('%Y-%m','2013-01')
        scraper.populate_tags()
    except (ScraperException, KeyboardInterrupt):
        pass
    scraper.close()

if __name__ == "__main__":
    main()
