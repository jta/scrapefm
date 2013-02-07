# -*- coding: utf-8 -*-
""" lastdb.py: Relational model for Last.fm scrape.
"""
import sqlite3
import peewee

DBASE = peewee.SqliteDatabase(None)

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

def load(dbname):
    """ Create tables if necessary """
    DBASE.init(dbname)
    DBASE.set_autocommit(False)
    DBASE.connect()
    Users.create_table(fail_silently=True)

    # we have dud artist to populate chart row
    # in the absence of scrobbles
    try:
        Artists.create_table()
        Artists.create(name = '') 
    except sqlite3.OperationalError:
        pass
    Tags.create_table(fail_silently=True)
    Friends.create_table(fail_silently=True)
    WeeklyArtistChart.create_table(fail_silently=True)
    ArtistTags.create_table(fail_silently=True)

    return DBASE
