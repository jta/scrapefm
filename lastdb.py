from peewee import *

dbase = SqliteDatabase(None)

class BaseModel(Model):
    class Meta:
        database = dbase

class User(BaseModel):
    id   = IntegerField(primary_key=True)
    name = CharField()
    age  = IntegerField(default=0)
    country = CharField(default='')
    gender  = CharField(default='')
    playcount = IntegerField()

class Friends(BaseModel):
    a = ForeignKeyField(User)
    b = ForeignKeyField(User)

def load(dbname):
    dbase.init(dbname)
    dbase.connect()
    User.create_table(fail_silently=True)
    Friends.create_table(fail_silently=True)
    return dbase



