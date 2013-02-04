import sqlite3
from peewee import *

dbase = SqliteDatabase(None)
char_max_length = 0

class BaseModel(Model):
    class Meta:
        database = dbase

class Users(BaseModel):
    id   = IntegerField(primary_key=True)
    name = CharField()
    age  = IntegerField(default=0)
    country = CharField(default='')
    gender  = CharField(default='')
    playcount = IntegerField(default=0)
    subscriber = BooleanField(default=False)

class Artists(BaseModel):
    name = TextField()
    playcount = IntegerField(default=0)
    tagcount = IntegerField(default=0)

class Tags(BaseModel):
    name = CharField()

class Friends(BaseModel):
    a = ForeignKeyField(Users)
    b = ForeignKeyField(Users)

class WeeklyArtistChart(BaseModel):
    weekfrom = CharField()
    user = ForeignKeyField(Users)
    artist = ForeignKeyField(Artists)
    playcount = IntegerField()

class ArtistTags(BaseModel):
    artist = ForeignKeyField(Artists)
    tag = ForeignKeyField(Tags)
    count = IntegerField()


def commit():
    dbase.commit()


def load(dbname):
    """ Create tables if necessary
    """
    dbase.init(dbname)
    dbase.set_autocommit(False)
    dbase.connect()
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


    return dbase
