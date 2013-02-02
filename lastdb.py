from peewee import *

dbase = SqliteDatabase(None)

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

class Friends(BaseModel):
    a = ForeignKeyField(Users)
    b = ForeignKeyField(Users)

class Artists(BaseModel):
    name = CharField()

class WeeklyArtistChart(BaseModel):
    weekfrom = CharField()
    uid = ForeignKeyField(Users)
    aid = ForeignKeyField(Artists)
    playcount = IntegerField()

def commit():
    dbase.commit()

def load(dbname):
    dbase.init(dbname)
    dbase.set_autocommit(False)
    dbase.connect()
    Users.create_table(fail_silently=True)
    Friends.create_table(fail_silently=True)
    Artists.create_table(fail_silently=True)
    WeeklyArtistChart.create_table(fail_silently=True)
    return dbase



