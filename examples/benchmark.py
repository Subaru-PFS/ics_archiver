#!/usr/bin/env python
from __future__ import print_function
from builtins import range
from builtins import object
import time
import opscore.protocols.parser as parser
import opscore.protocols.keys as keys
import opscore.protocols.types as types
import opscore.protocols.messages as messages
from opscore.utility.astrotime import AstroTime,TAI
import archiver.table as table

repeat = 10000
#dbengine = 'none'
#dbengine = 'mysql'
dbengine = 'postgres'
config = ''
preParsed = False

if dbengine == 'mysql':
    import MySQLdb as dbapi
    db = dbapi.connect(user="testing",passwd="sdss3",db="test")
    config = 'set storage_engine=InnoDB'
elif dbengine == 'postgres':
    import pyPgSQL.PgSQL as dbapi
    db = dbapi.connect(user="testing",password="sdss3",database="test")
    config = 'set session synchronous_commit to off'
else:
    class LazyCursor(object):
        def execute(self,cmd): pass
        def close(self): pass
    class LazyDatabase(object):
        def cursor(self): return LazyCursor()
        def commit(self): pass
        def close(self): pass
    class dbapi(object):
        apilevel,threadsafety,paramstyle = None,None,None
    db = LazyDatabase()

print('Using DB engine:',dbengine)
print('API level, Thread safety, Param style =',dbapi.apilevel,dbapi.threadsafety,dbapi.paramstyle)
print('config:',config)

replyParser = parser.ReplyParser()

kdict = keys.KeysDictionary.load('testing')

cursor = db.cursor()
if config:
    cursor.execute(config)
    db.commit()

table.SQLTable.commit = db.commit
table.SQLTable.execute = cursor.execute
#Table = table.Blackhole
#Table = table.BufferedTable
#Table = table.PostgreSQLBufferedTable
Table = table.PostgreSQLBinaryBufferedTable
#Table = table.MySQLBufferedTable
bufsize = 1000

raw = Table('raw',(
    types.UInt(name='id'),
    types.Double(name='tai'),
    types.String(name='msg')
),nrows=bufsize)
raw.create(indices=('tai',))
id,tai,msg = raw.columns

reply = Table('reply',(
    types.UInt(name='raw_id'),
    types.UInt(name='actor_id'),
    types.String(name='program'),
    types.String(name='username'),
    types.UInt(name='cmd_num'),
    messages.ReplyHeader.MsgCode
),nrows=bufsize)
reply.create(indices=('actor_id','cmd_num'))
raw_id,actor_id,program,username,cmd_num,code = reply.columns

keyTable = {
    'testing_count': Table('testing_count',
        (types.UInt(name='raw_id'),types.UInt(name='value1')),
        nrows=bufsize
    ),
    'testing_time': Table('testing_time',
        (types.UInt(name='raw_id'),types.Double(name='value1'),types.String(name='value2')),
        nrows=bufsize
    )
}
keyTable['testing_count'].create()
keyTable['testing_time'].create()
db.commit()

begin = time.time()

for count in range(repeat):
    # fake a received message
    if not preParsed or not count:
        now = AstroTime.now(tz=TAI)
        recvdMsg = msg("prog.user 911 testing I count=%d;time=%.3f,'%s'" % (count,time.time(),now))
    # save the raw message
    theID = id(count)
    raw.insert(theID,tai(now.MJD()*86400.),recvdMsg)
    # parse the message
    if not preParsed or not count:
        parsed = replyParser.parse(recvdMsg)
        # save the reply
        hdr = parsed.header
    reply.insert(
        theID,actor_id(123),program(hdr.program),username(hdr.user),
        cmd_num(hdr.commandId),hdr.code
    )
    # validate and save each keyword
    for keyword in parsed.keywords:
        # look for a corresponding keyword validator
        if not preParsed or not count:
            key = kdict[keyword.name]
            key.consume(keyword)
        table = '%s_%s' % (hdr.actor,keyword.name)
        keyTable[table].insert(theID,*tuple(keyword.values))

end = time.time()
elapsed = end - begin
print('%d columns written in %.3f secs: rate = %.2f kHz' % (repeat,elapsed,1e-3*repeat/elapsed))

cursor.close()
db.close()
