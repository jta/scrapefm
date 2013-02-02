#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: João Taveira Araújo (first second at gmail / @jta)
"""
import argparse
import collections
import lastdb
import logging
import misc
import pylast
import os

API_KEY_VAR = "LAST_FM_API_PKEY"

logger = logging.getLogger('scrapefm')

class Scraper(object):
    """ Use Last.fm API to retrieve data to local database. """
    def __init__(self, api_key):
        self.network = pylast.LastFMNetwork(api_key = api_key)
        self.network.enable_caching()

    def scrape(self, seed, maxlimit = 5):
        """ Start from seed user and scrape info by following social graph.
        """
        # set of inspected usernames
        visited   = dict([ (u.name, u.id) for u in lastdb.User.select() ])
        # list of users (obj) yet to be visited
        # all names must be converted to lowercase for comparison
        unvisited = set([seed])

        while len(visited) < maxlimit:
            username = unvisited.pop()
            if username not in visited:
                visited[username] = self._scrape_user(username)
            for new in self._friend_discovery(username, visited):
                unvisited.add(new)
            logger.debug("%d unvisited users.", len(unvisited))
                
    def _friend_discovery(self, username, users, maxfriends = 500):
        """ Given user, explore connected nodes.
            If a neighbour node has already been seen, add edge to friend table.
            Otherwise, return unexplored neighbour.
        """
        user = self.network.get_user(username)
        for friend in user.get_friends(maxfriends):
            friendname = friend.name
            if friendname in users:
                logger.debug("Connecting %s with %s.", username, friendname)
                lastdb.Friends.create( a = users[username], b = users[friendname])
            else:
                yield friendname

    def _scrape_user(self, username):
        """ Scrapes all info associated to username into database
        """
        user   = self.network.get_user(username)

        # fields in lastdb.User table maps to respective get function
        # in pylast.User class
        values = {}
        for field in lastdb.User._meta.get_fields():
            values[field.name] = getattr(user, 'get_%s' % field.name)() 

        # annoyingly get_country returns class rather than string.
        # if None, let it default to empty string.
        if not values['country'].get_name():
            del values['country']

        logger.debug("Creating user %s.", user)
        lastdb.User.create( **values )
        return user.get_id()


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

    # XXX: start database from scratch each time for now
    try:
        os.remove(args.db)
    except OSError:
        pass

    return args

def main():
    """ Main entry point
    """
    args    = parse_args()

    lastdb.load(args.db)
    scraper = Scraper(args.api_key)
    scraper.scrape('RJ')

if __name__ == "__main__":
    main()
