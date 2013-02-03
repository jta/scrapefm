#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: João Taveira Araújo (first second at gmail / @jta)
"""
import argparse
from datetime import datetime
import lastdb
import logging
import pylast
import os
import random

API_KEY_VAR = "LAST_FM_API_PKEY"

LOGGER = logging.getLogger('scrapefm')

class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    def __init__(self, api_key):
        self.network = pylast.LastFMNetwork(api_key = api_key)
        self.network.enable_caching()
        self.errnum = 3

    def _friend_discovery(self, username, users, maxfriends = 500):
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
            print date.strftime(datefmt)
            if date.strftime(datefmt) == datematch:
                weeklist.append( (weekfrom, weekto) )
        return weeklist

    def _scrape(self, func, args):
        """ Wrapper which handles pylast.* exceptions
        """
        try:
            func(*args)
            lastdb.commit()
        except (pylast.WSError, 
                pylast.NetworkError, 
                pylast.MalformedResponseError) as e:
            self.error(e)

    def _scrape_artist(self, name):
        LOGGER.info("Found new artist: %s.", name)
        assert not lastdb.Artists.select().where(lastdb.Artists.name == name).exists()
        return lastdb.Artists.create(name = name).id

    def _scrape_user(self, username):
        """ Scrapes all info associated to username into database.
            Returns user id.
        """
        user   = self.network.get_user(username)

        # fields in lastdb.Users table maps to respective get function
        # in pylast.Users class
        values = {}
        for field in lastdb.Users._meta.get_fields():
            values[field.name] = getattr(user, 'get_%s' % field.name)() 

        # annoyingly get_country returns class rather than string.
        if not values['country'].get_name():
            values['country'] = None

        # filter out fields with no values and let defaults take over.
        values = dict((k, v) for k, v in values.items() if v)
        LOGGER.debug("Adding user %s.", user)
        return lastdb.Users.create( **values ).id

    def _scrape_users(self, username, users, neighbours):
        """ Wrapped function to store user and explore neighbours """
        LOGGER.info("Processing username %s.", username)
        if username not in users:
            users[username] = self._scrape_user(username)
        for new in self._friend_discovery(username, users):
            neighbours.add(new)

    def _scrape_week(self, user, userid, weekfrom, weekto, artistcache):
        """ Scrape single week, inserting artists if not cached.  """
        LOGGER.debug("Scraping week %s for user %s.", weekfrom, user.name)

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
               
    def _scrape_weeks(self, username, userid, weeklist, artistcache):
        """ Populating weekly charts for single user """
        user = self.network.get_user(username)
        weeksdone = set()
        try:
            rows = lastdb.WeeklyArtistChart.select()\
                            .where(lastdb.WeeklyArtistChart.user == userid)
            weeksdone.update([row.weekfrom for row in rows])
        except lastdb.WeeklyArtistChart.DoesNotExist:
            pass

        for weekfrom, weekto in weeklist:
            if weekfrom not in weeksdone:
                self._scrape_week(user, userid, weekfrom, weekto, artistcache)

    def error(self, errmsg):
        """ Print error message and keep count of errors.
            If threshold exceeded, quit.
        """
        self.errnum -= 1
        LOGGER.error(errmsg)
        lastdb.dbase.rollback()
        if not self.errnum:
            LOGGER.warning("Exiting due to excess errors.")
            raise Exception

    def populate_charts(self, datefmt, datematch):
        """ From user list in db, import weekly charts for matching dates.
            Will keep try to re-import weeks which do not exist, i.e. because
            users did not scrobble.
        """
        artists  = dict([ (a.name, a.id) for a in lastdb.Artists.select() ])
        weeklist = self._get_weeks(datefmt, datematch)
        
        for dbuser in lastdb.Users.select():
            self._scrape( self._scrape_weeks, (dbuser.name, dbuser.id, weeklist, artists) )

    def populate_tags(self):
        """ Insert all tags for all artists """
        pass

    def populate_users(self, seed, maxlimit = 100000):
        """ Start from seed user and scrape info by following social graph.
        """
        # set of inspected usernames
        visited   = dict([ (u.name, u.id) for u in lastdb.Users.select() ])
        # set of users (obj) yet to be visited
        unvisited = set([seed])

        while len(visited) < maxlimit:
            username = unvisited.pop()
            while username in visited and len(unvisited):
                username = unvisited.pop()
            self._scrape( self._scrape_users, (username, visited, unvisited) )


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

    parser.add_argument('--debug', dest='debug',
                        action='store_true', default=False,
                        help='print lots of debug information')

    parser.add_argument("db", type=str,
                        help="Database to be written to.")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not args.api_key:
        raise Exception

    return args

def main():
    """ Main entry point
    """
    args    = parse_args()

    lastdb.load(args.db)
    scraper = Scraper(args.api_key)
    try:
        scraper.populate_users('RJ', 10)
        scraper.populate_charts('%Y-%W','2013-01')
        scraper.populate_tags()
    except:
        lastdb.dbase.rollback()
        lastdb.dbase.close()

if __name__ == "__main__":
    main()
