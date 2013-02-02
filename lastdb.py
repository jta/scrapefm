from peewee import *

dbase = SqliteDatabase(None)

class BaseModel(Model):
    class Meta:
        database = dbase

class User(BaseModel):
    id   = IntegerField(primary_key=True)
    name = CharField()
    age  = IntegerField()
    country = CharField()
    gender  = CharField()
    playcount = IntegerField()

class Friends(BaseModel):
    a = ForeignKeyField(User, related_name='friends')
    b = ForeignKeyField(User)

def load(dbname):
    dbase.init(dbname)
    dbase.connect()
    User.create_table()
    Friends.create_table()
    return dbase



