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
import re
import types

API_KEY_VAR = "LAST_FM_API_PKEY"

LOGGER = logging.getLogger('scrapefm')

class ScraperException(Exception):
    """ Raised if too many internal errors from Last.fm API """

class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    ERRLIM = 10     # errors before quitting
    COMLIM = 100    # outstanding transactions before commit

    def __init__(self, options):
        self.__dict__ = dict(options.__dict__.iteritems())
        self.network = pylast.LastFMNetwork(api_key = self.api_key)
        self.network.enable_caching()
        self.errcnt = 0
        self.commit = 0

        self._load()

    def _load(self):
        """ Load information from database. """
        self.db    = lastdb.load(self.db)

        # get all possible weeks matching desired pattern.
        self.weeks = list(self._get_weeks(self.network.get_user('RJ'), 
                                          self.datefmt,
                                          self.datematch))

        # name -> id mapping for all scraped users and artists.
        self.users   = dict([ (u.name, u.id) for u in lastdb.Users.select() ])
        self.artists = dict([ (a.name, a.id) for a in lastdb.Artists.select() ])

        

        """
           try:
                    rows = lastdb.WeeklyArtistChart.select()\
                               .where(lastdb.WeeklyArtistChart.user == userid)
                    weeksdone.update([row.weekfrom for row in rows])
                except lastdb.WeeklyArtistChart.DoesNotExist:
                    pass
        """

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

    def _get_friends(self, user):
        return [ friend.name for friend in user.get_friends(self.maxfriends) ]
    
    @classmethod
    def _get_weeks(self, user, datefmt, datematch):
        """ Get weeks whose starting date matches provided pattern. """
        pattern = re.compile(datematch)
        unix_to_date = lambda x: datetime.fromtimestamp(x).strftime(datefmt)
        for weekfrom, weekto in user.get_weekly_chart_dates():
            if pattern.match( unix_to_date(int(weekfrom)) ):
                yield weekfrom, weekto

    def scrape_artist(self, name):
        LOGGER.info("Found new artist: %s.", name)
        assert not lastdb.Artists.select()\
                        .where(lastdb.Artists.name == name).exists()
        return lastdb.Artists.create(name = name).id

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
                lastdb.Friends.get( **ordered )
            except lastdb.Friends.DoesNotExist:
                LOGGER.debug("Connecting %s with %s.", *ordered.values())
                lastdb.Friends.create( **ordered )

    @_commit_or_roll
    def scrape_user(self, username):
        """ Scrape user info and return ID. """
        LOGGER.info("Adding user %s.", username)
        user   = self.network.get_user(username)
        
        # hardwire function so that fields in lastdb.Users table map
        # to get_* function in pylast.Users class
        user.get_subscriber = user.is_subscriber
        values = {}
        for field in lastdb.Users._meta.get_fields():
            values[field.name] = field.db_value(getattr(user, 'get_%s' % field.name)())

        userid  = lastdb.Users.create( **values ).id

        # link neighbours
        if self.do_connect:
            self.scrape_friends(user, userid)

        # get weekly chart
        for weekfrom, weekto in self.weeks:
            self.scrape_week(user, userid, weekfrom, weekto)
        
        return userid

    def scrape_week(self, user, userid, weekfrom, weekto):
        """ Scrape single week, inserting artists if not cached.  """
        LOGGER.debug("Scraping week %s for user %s.", weekfrom, user)

        row = { 'weekfrom': weekfrom, 'user': userid }

        for artist, count in user.get_weekly_artist_charts(weekfrom, weekto):
            if artist.name not in self.artists:
                self.artists[artist.name] = self.scrape_artist(artist.name)
            row.update({'artist': self.artists[artist.name], 'playcount': count})
            lastdb.WeeklyArtistChart.create( **row )

        if 'artist' not in row:
            LOGGER.debug("No chart, adding empty entry as placeholder.")
            row.update({'artist': self.artists[''], 'playcount': 0})
            lastdb.WeeklyArtistChart.create( **row )
               
    def close(self):
        """ Close database, rolling back any uncommitted changes. """
        self.db.commit()
        self.db.close()

    def run(self):
        """ Start from seed user and scrape info by following social graph.
        """
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
                scraped[username] = self.scrape_user(username)


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
    options.do_connect = True
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
