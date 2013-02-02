#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: João Taveira Araújo (first second at gmail / @jta)
"""
import argparse
import lastdb
import logging
import os

API_KEY_VAR = "LAST_FM_API_PKEY"

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

if __name__ == "__main__":
    main()
