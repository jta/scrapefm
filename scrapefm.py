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

API_KEY_VAR = "LAST_FM_API_PKEY"

LOGGER = logging.getLogger('scrapefm')

class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    def __init__(self, api_key):
        self.network = pylast.LastFMNetwork(api_key = api_key)
        self.network.enable_caching()

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

    def _scrape_artist(self, name):
        assert not lastdb.Artists.select().where(lastdb.Artists.name == name).exists()

        LOGGER.debug("Inserting new artist: %s.", name)
        artist = lastdb.Artists.create(name = name)
        return artist.id

    def _scrape_user(self, username):
        """ Scrapes all info associated to username into database
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
        LOGGER.debug("Creating user %s.", user)
        lastdb.Users.create( **values )
        return user.get_id()

    def _scrape_week(self, user, uid, weekfrom, weekto, artistcache):
        LOGGER.debug("Scraping week from %s for user %s.", weekfrom, user.name)

        for name, playcount in user.get_weekly_artist_charts(weekfrom, weekto):
            if name not in artistcache:
                artistcache[name] = self._scrape_artist(name)

            lastdb.WeeklyArtistChart.create( weekfrom = weekfrom,
                                             uid = uid,
                                             aid = artistcache[name],
                                             playcount = playcount )
               
    def populate_charts(self, datefmt, datematch):
        """ From user list in db, import weekly charts for matching dates.
        """
        artistcache = dict([ (a.name, a.id) for a in lastdb.Artists.select() ])

        for dbuser in lastdb.Users.select():
            user = self.network.get_user(dbuser.name)
            for weekfrom, weekto in user.get_weekly_chart_dates():
                date = datetime.fromtimestamp(int(weekfrom))
                if date.strftime(datefmt) != datematch:
                    continue
                try:
                    lastdb.WeeklyArtistChart.get(   uid = dbuser.id, 
                                                    weekfrom = weekfrom)
                except lastdb.WeeklyArtistChart.DoesNotExist:
                    self._scrape_week(  user, dbuser.id, 
                                        weekfrom, weekto, artistcache)
            lastdb.commit()

    def populate_tags(self):
        pass

    def populate_users(self, seed, maxlimit = 100000):
        """ Start from seed user and scrape info by following social graph.
        """
        # set of inspected usernames
        visited   = dict([ (u.name, u.id) for u in lastdb.Users.select() ])
        # list of users (obj) yet to be visited
        unvisited = set(visited.keys())
        unvisited.add(seed)

        while len(visited) < maxlimit:
            username = unvisited.pop()
            LOGGER.debug("Processing username %s.", username)
            if username not in visited:
                visited[username] = self._scrape_user(username)
            for new in self._friend_discovery(username, visited):
                unvisited.add(new)
            lastdb.commit()




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
    scraper.populate_users('RJ', 10)
    scraper.populate_charts('%Y-%m','2012-12')
    scraper.populate_tags()

if __name__ == "__main__":
    main()
