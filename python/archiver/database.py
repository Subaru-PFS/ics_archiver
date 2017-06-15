"""
Archiver interface to database storage
"""

# Created 01-Mar-2009 by David Kirkby (dkirkby@uci.edu)

import os,os.path,string,time

from twisted.internet import defer
from twisted.python import log

from opscore.protocols import types,messages
from opscore.utility import astrotime
import actors

class DatabaseException(Exception):
    pass

"""
The mapping between storage types defined in opscore.protocols.types and
generic SQL column types. These can be customized for different SQL
implementations below.
"""
sqlTypes = {
    'int2': 'smallint',
    'int4': 'integer',
    'int8': 'bigint',
    'flt4': 'real',
    'flt8': 'double precision',
    'text': 'text'
}

def init(options):
    """
    Initializes the specified database product.
    """
    # create the buffer file path
    Table.bufferPath = options.tmpPath
    
    # check for a valid options.dbEngine and set engine-specific parameters
    global sqlTypes
    host,user,pw,db = options.dbHost,options.dbUser,options.dbPassword,options.dbName
    if options.dbEngine == 'postgres':
        dbModule = 'psycopg2'
        connectionArgs = { 'user':user, 'password':pw, 'host':host, 'database':db }
        scanTables = "select tablename from pg_tables where tableowner='%s'" % user
        Table.insertStatement = string.Template("COPY $table FROM '$file' CSV QUOTE ''''")
    elif options.dbEngine == 'mysql':
        dbModule = 'MySQLdb'
        connectionArgs = { 'user':user, 'passwd':pw, 'host':host, 'db':db }
        Table.insertStatement = string.Template(
            "LOAD DATA INFILE '$file' INTO TABLE $table FIELDS " +
            "TERMINATED BY ',' ENCLOSED BY ''''")
    elif options.dbEngine == 'none':
        print 'will not use any database engine'
    else:
        raise DatabaseException('Unknown engine: %s' % options.dbEngine)
        
    # remember the KeyTable buffer size
    KeyTable.bufferSize = options.keyBufferSize
    
    # use UTC for timestamps when the system clock is actually tracking TAI
    if options.systemClock == 'TAI':
        KeyTable.timestampTZ = astrotime.UTC
    else:
        KeyTable.timestampTZ = astrotime.TAI
    
    # process the list of tables to trace activity on (--trace-list option)
    Table.traceList = [ ]
    aliases = { 'raw': 'reply_raw', 'hdr': 'reply_hdr', 'actors': actors.Actor.tableName }
    for traceTarget in options.traceList.lower().split(','):
        if traceTarget in aliases:
            Table.traceList.append(aliases[traceTarget])
        else:
            Table.traceList.append(traceTarget.replace('.','__'))            

    # the remaining initialization requires a database engine
    if options.dbEngine == 'none':
        Table.connectionPool = None
        return
        
    # try to load the engine's DBAPI module
    import twisted.python.reflect
    print 'importing %s dbapi module %s' % (options.dbEngine,dbModule)
    dbapi = twisted.python.reflect.namedModule(dbModule)

    # open a database connection for initialization commands
    db = dbapi.connect(**connectionArgs)
    cursor = db.cursor()

    # scan the list of user tables already defined in the database
    tables = db.cursor()
    tables.execute(scanTables)
    for tableInfo in tables.fetchall():
        # SQL identifiers are generally case insensitive so lower-case everything
        tableName = tableInfo[0].lower()
        if tableName.startswith('sql_') or tableName.startswith('pg_'):
            continue
        # lookup the number of rows already stored in this table
        #cursor.execute("select count(*) from %s" % tableName)
        idLab = 'id' if tableName in ["reply_raw", "actors"] else 'raw_id'
        cursor.execute("select max(%s) from %s" % (idLab, tableName))
        tableRows = cursor.fetchone()[0]
        if tableRows is None:
            tableRows = 0
        print "database: table %s contains %d rows" % (tableName,tableRows)
        # get a list of this table's column names
        cursor.execute("select * from %s where 1=0" % tableName)
        columnNames = [ ]
        for column in cursor.description:
            columnNames.append(column[0].lower())
        Table.existing[tableName] = (tableRows,columnNames)
    
    # scan the table of known actors (if one is present)
    if actors.Actor.tableName in Table.existing:
        cursor.execute('select * from %s' % actors.Actor.tableName)
        for (idnum,name,major,minor,cksum) in cursor.fetchall():
            # a newer version will supercede older ones in the existing dictionary
            actors.Actor.existing[name] = (idnum,major,minor,cksum)
    else:
        print "database: no actors defined"
    for actorName in sorted(actors.Actor.existing.iterkeys()):
        (idnum,major,minor,cksum) = actors.Actor.existing[actorName]
        print "database: expecting %s actor at version %d.%d" % (actorName,major,minor)

    # close the initialization database connection
    cursor.close()
    db.close()
    
    # initialize the twisted connection pool
    import twisted.enterprise.adbapi
    Table.connectionPool = (
        twisted.enterprise.adbapi.ConnectionPool(dbModule,**connectionArgs))
    
    # Install a shutdown callback that will flush any buffered data.
    from twisted.internet import reactor
    reactor.addSystemEventTrigger('after','shutdown',shutdown,dbapi,**connectionArgs)
    
    # Install a startup callback that initializes the core database tables.
    # We use a callback for this so that we can use the reactor and dbapi connection pool.
    reactor.addSystemEventTrigger('after','startup',initCoreTables,
        options.rawBufferSize,options.hdrBufferSize)

def initCoreTables(rawBufferSize,hdrBufferSize):
    """
    Initializes the core database tables
    
    This function is called after the reactor is started but before entering the
    main event loop.
    """
    Table.attach('reply_raw',(
        types.Long(name='id'),
        types.Double(name='tai'),
        types.String(name='msg')
    ),bufferSize=rawBufferSize,indices=('tai',))
    Table.attach('reply_hdr',(
        types.Long(name='raw_id'),
        types.UInt(name='actor_id'),
        types.String(name='program'),
        types.String(name='username'),
        types.UInt(name='cmd_num'),
        messages.ReplyHeader.MsgCode,
        types.UInt(name='key_errors')
    ),bufferSize=hdrBufferSize,indices=('actor_id',))


def shutdown(dbapi,**connectionArgs):
    print 'database: starting shutdown sequence'
    db = dbapi.connect(**connectionArgs)
    cursor = db.cursor()
    # close out each table in turn
    for table in Table.registry.itervalues():
        print 'database: flushing %d rows to %s' % (len(table.rowBuffer),table.name)
        table.bufferFile.close()
        if len(table.rowBuffer) > 0:
            statement = Table.insertStatement.substitute(
                file=table.bufferFileName,table=table.name)
            cursor.execute(statement)
        os.unlink(table.bufferFileName)
        # close any open trace
        table.trace(enable=False)
    cursor.close()
    db.commit()
    db.close()
    print 'database: shutdown complete'

def executeSQL(transaction,statements,table):
    """
    Executes a sequence of SQL statements
    """
    for statement in statements:
        transaction.execute(statement)
    return table

def loadFile(transaction,bufferFile,table):
    """
    Loads the contents of an ASCII file into a database table
    
    This function runs in a separate thread and so must not depend on
    any table attributes that might be modified elsewhere. In
    particular, we only read table.name here and do not modify any table
    attributes.
    """
    bufferFileName = bufferFile.name
    bufferFile.close()
    statement = Table.insertStatement.substitute(
        file=bufferFileName,table=table.name)
    try:
        transaction.execute(statement)
        os.unlink(bufferFileName)
    except Exception, e:
        log.err('database.loadFile failed for %s with error: %s (see below for details)'
            % (bufferFileName,e.__class__.__name__))
        log.err(str(e))
    return table
    
def ping(options):
    """
    Performs periodic database maintenance
    """
    if not Table.lastActivity:
        return
    now = time.time()
    idleTime = now - Table.lastActivity
    # print 'Last activity was %.3f secs ago' % idleTime
    if idleTime < options.idleTime:
        return
    # find table that has been idle longest
    maxIdler = None
    maxIdleTime = 0
    for table in Table.registry.itervalues():
        idleTime = now - table.lastActivity
        if idleTime > maxIdleTime and len(table.rowBuffer) > 0 and not table.busy:
            maxIdler = table
            maxIdleTime = idleTime
    if maxIdler and not maxIdler.busy:
        print 'Flushing table %s idle for %.3f secs' % (maxIdler.name,idleTime)
        maxIdler.flushBuffer()
        maxIdler.openBuffer()

class Table(object):
    
    connectionPool = None
    bufferPath = None
    insertStatement = None

    existing = { }
    registry = { }

    lastActivity = None

    @staticmethod
    def release(table):
        """
        Clears this table's busy flag
        
        Normally invoked as a twisted Deferred callback.
        """
        if table.traceEnable:
            print >>table.traceFile, "OUT %d %f" % (
                table.traceOut,time.time()-table.traceStart)
        table.busy = False
        
    @staticmethod
    def prepareColumnNames(columnTypes):
        """
        Returns a tuple of three lists: column aliases, full names, and value types.
        
        The returned list of value types differs from the input column types in that
        each returned type is a fundamental (ie, not repeated or compound) type with a
        one-to-one correspondence with a table column.
        """

        appendTypes = False
        
        idNames = [ ]
        colNames = [ ]
        valueTypes = [ ]
        for index,col in enumerate(columnTypes):
            if isinstance(col,types.RepeatedValueType):
                if not hasattr(col.vtype,'storage'):
                    raise DatabaseException(
                        "No storage type specified for repeated column: %s" % col)
                storage = col.vtype.storage.lower()
                name = (getattr(col.vtype,'name') or ('val%d' % index)).lower()
                # A repeated type is stored using a fixed number of columns. In case
                # there is no maximum number of repeats specified, only the minimum
                # number will be stored in the database.
                repeat = col.maxRepeat or col.minRepeat
                for repIndex in xrange(repeat):
                    if col.minRepeat == 1 and col.maxRepeat == 1:
                        # don't number a value that is only repeated once
                        repName = name
                    else:
                        repName = '%s_%d' % (name,repIndex)
                    idNames.append(repName)
                    if appendTypes:
                        colNames.append('%s__%s' % (repName,storage))
                    else:
                        colNames.append(repName)
                    valueTypes.append(col.vtype)
            elif isinstance(col,types.CompoundValueType):
                name = (getattr(col,'name') or ('val%d' % index)).lower()
                for subindex,vtype in enumerate(col.vtypes):
                    if not hasattr(vtype,'storage'):
                        raise DatabaseException(
                            "No storage type specified for compound column: %s" % col)
                    storage = vtype.storage.lower()
                    subname = ("%s_%s" %
                        (name,(getattr(vtype,'name') or ('val%d' % subindex)).lower()))
                    idNames.append(subname)
                    if appendTypes:
                        colNames.append('%s__%s' % (subname,storage))
                    else:
                        colNames.append(subname)

                    valueTypes.append(vtype)
            else:
                if not hasattr(col,'storage'):
                    raise DatabaseException("No storage type specified for column: %s" % col)
                name = (getattr(col,'name') or ('val%d' % index)).lower()
                idNames.append(name)
                if appendTypes:
                    colNames.append('%s__%s' % (name,col.storage.lower()))
                else:
                    colNames.append(name)
                    
                valueTypes.append(col)
        return (idNames,colNames,valueTypes)

    @staticmethod
    def attach(name,columnTypes=None,bufferSize=None,indices=None,tableClass=None):
        """
        Attaches a database table, creating it if necessary
        """
        name = name.lower()
        if name in Table.registry:
            table = Table.registry[name]
            # check for compatible columns (remove this as a speed optimization?)
            if columnTypes:
                aliases,colNames,valueTypes = Table.prepareColumnNames(columnTypes)
                if table.columnNames != colNames:
                    raise DatabaseException(
                        "Incompatible column definitions for %s:\nNEW: %s\nOLD: %s" %
                        (name,colNames,table.columnNames)
                    )
            return table
        else:
            if not columnTypes:
                raise DatabaseException(
                    "Cannot create table without column types: %s" % name)
            # by default, create a new instance of a Table, but allow subclasses to
            # customize this behaviour
            if not tableClass:
                tableClass = Table
            return tableClass(name,columnTypes,bufferSize,indices)
    
    def __init__(self,name,columnTypes,bufferSize=None,indices=None):
        """
        Initializes a new database table.
        
        Once a table is initialized, it is ready to have rows appended.
        If the table does not already exist in the database, it will be
        created. If the table already exists but with incompatible
        column definitions, a DatabaseException will be raised. The
        columnTypes should be a tuple of types defined in
        opscore.protocols.types. The indices parameter is only used if a
        table needs to be created in the database and should be a tuple
        of column type names.
        """
        self.name = name.lower()
        self.columnTypes = columnTypes
        self.busy = False
        self.traceEnable = False
        # convert the coulumn types into a list of SQL column names
        self.aliases,self.columnNames,self.columnFinalTypes = (
            Table.prepareColumnNames(columnTypes))
        # does the database already contain a table with this name?
        if self.name in Table.existing:
            (nRows,existingColumnNames) = Table.existing[self.name]
            # check that the declared column types are compatible with the existing table
            if self.columnNames != existingColumnNames:
                raise DatabaseException(
                    "Incompatible column definitions for %s:\nNEW: %s\n DB: %s" %
                    (self.name,self.columnNames,existingColumnNames)
                )
            self.nRows = nRows
            print 'initializing exisiting table %s' % self.name
        else:
            # create a new empty table
            self.nRows = 0
            print 'creating new table %s with %r' % (self.name,self.columnNames)
            self.create(indices)
        # initialize this table's memory and file buffers
        try:
            self.bufferSize = int(bufferSize)
        except ValueError,TypeError:
            raise DatabaseException("Invalid buffer size for table %s: %r" % 
                (self.name,bufferSize))
        if self.bufferSize <= 0:
            raise DatabaseException("Buffer size must be positive for table %s: %s" %
                (self.name,bufferSize))
        self.nFlushes = 0
        self.openBuffer()
        # are we tracing this table's activity?
        if self.name in Table.traceList:
            self.trace()
        # record this newly initialized table in our registry
        Table.registry[self.name] = self
        self.recordActivity()

    def recordActivity(self):
        """
        Records the time of last activity for a table
        """
        self.lastActivity = time.time()
        Table.lastActivity = self.lastActivity

    def trace(self,enable=True):
        """
        Enables or disables tracing of table writes
        """
        if not self.traceEnable and enable:
            # turn tracing on
            print 'Start trace on table',self.name
            self.traceEnable = True
            self.traceFile = open(os.path.join(Table.bufferPath,'%s.trace' % self.name),'w')
            self.traceRows = self.nRows
            self.traceOut = 0
            self.traceStart = time.time()
            print >>self.traceFile, "START %f" % self.traceStart
            print >>self.traceFile, "IN 0 0.0"
        elif self.traceEnable and not enable:
            # turn tracing off
            print 'Stop trace on table',self.name
            self.traceEnable = False
            self.traceFile.close()

    def openBuffer(self):
        self.rowBuffer = [ ]
        self.bufferFileName = os.path.join(
            Table.bufferPath,'%s_%d' % (self.name,self.nFlushes))
        self.bufferFile = open(self.bufferFileName,'w')
        
    def flushBuffer(self):
        print '%s: flushing %d rows' % (self.name,len(self.rowBuffer))
        self.nFlushes += 1
        self.busy = True
        if self.traceEnable:
            print >>self.traceFile, "OUT %d %f" % (
                self.traceOut,time.time()-self.traceStart)
            self.traceOut += len(self.rowBuffer)
        if Table.connectionPool:
            Table.connectionPool.runInteraction(
                loadFile,self.bufferFile,self).addCallback(Table.release)
        else:
            self.bufferFile.close()
            Table.release(self)

    def record(self,*rowValues):
        """
        Records one new row of values
        """
        rowString = ''
        for index,colName in enumerate(self.columnNames):
            storage = self.columnFinalTypes[index].storage.lower()
            # We might have fewer values than columns if the last columnType is
            # repeated with variable length.
            try:
                value = rowValues[index]
            except IndexError:
                value = types.InvalidValue
            if index:
                rowString += ','
            if value is types.InvalidValue:
                # leave this field empty to signal a NULL SQL value
                pass
            elif hasattr(value,'storageValue'):
                rowString += value.storageValue()
            elif storage == 'text':
                rowString += "'%s'" % value.replace("'","''")
            elif storage[:3] == 'int':
                if isinstance(type(value),types.UInt) and (value & 0x80000000):
                    # interpret the MSB as a sign bit to encode a UInt as as an Int
                    encoded = -(int(value)&0x7fffffff)
                    rowString += str(encoded)
                else:
                    rowString += str(int(value))
            elif storage[:3] == 'flt':
                rowString += repr(float(value))
        print >> self.bufferFile, rowString
        self.rowBuffer.append(rowValues)
        self.nRows += 1
        # record the time of this table activity
        self.recordActivity()
        # trace this table write
        if self.traceEnable:
            print >>self.traceFile, "IN %d %f" % (
                self.nRows-self.traceRows,self.lastActivity-self.traceStart)
        # flush this table now?
        if len(self.rowBuffer) >= self.bufferSize:
            if self.busy:
                print 'delaying flush of %d rows to %s' % (len(self.rowBuffer),self.name)
            else:
                self.flushBuffer()
                self.openBuffer()

    def create(self,indices=None):
        """
        Creates this table
        
        The first column is declared as the primary key. An index will
        also be created for any named columns in indices. This method
        returns after queueing the actual SQL statement execution to
        another thread.
        """
        global sqlTypes
        statements = [ ]
        sql = 'create table %s (' % self.name
        for index,colName in enumerate(self.columnNames):
            storage = self.columnFinalTypes[index].storage.lower()
            if storage not in sqlTypes:
                raise DatabaseException('database: unsupported storage type: %s' % storage)
            sqlType = sqlTypes[storage]
            if index:
                sql += ',%s %s' % (colName,sqlType)
            else:
                sql += '%s %s primary key' % (colName,sqlType)
        sql += ')'
        statements.append(sql)
        # create any requested secondary indices on this table
        names = indices or [ ]
        for name in names:
            if name not in self.aliases:
                raise DatabaseException('Invalid index column name: %s' % name)
            colName = self.columnNames[self.aliases.index(name)]
            statements.append('create index %s_%s on %s(%s)'
                % (self.name,colName,self.name,colName))
        self.busy = True
        if Table.connectionPool:
            Table.connectionPool.runInteraction(
                executeSQL,statements,self).addCallback(Table.release)
        else:
            Table.release(self)

def keyTableFetch(transaction,sql,vtypes,data = None):
    """
    Starts a database transaction to load rows from a key table
    
    Runs in a separate thread using the twisted dbapi connection pool. Any existing
    row data passed in will be appended to when the query completes.
    """
    if data is None:
        data = [ ]
    print 'keyTableFetch >>>',sql
    try:
        transaction.execute(sql)
        print 'transaction finished'
        for rowRaw in transaction.fetchall():
            # the first value is always a TAI timestamp in MJD seconds
            rowTyped = [ astrotime.AstroTime.fromMJD(rowRaw[0]/86400.,astrotime.TAI) ]
            for vtype,value in zip(vtypes[1:],rowRaw[1:]):
                if value is None:
                    rowTyped.append(types.InvalidValue)
                else:
                    rowTyped.append(vtype(value))
            data.append(rowTyped)
        print 'data appended'
        return data
    except:
        # since this executes in a separate thread, we won't see exceptions unless
        # we explicitly catch them like this
        log.err()
        return data

class KeyTable(Table):
    """
    Stores the values associated with a specific keyword in a database table
    """
    @staticmethod
    def name(actorName,keyName):
        """
        Returns the table name corresponding to actor.keyword
        """
        return '%s__%s' % (actorName.lower(),keyName.lower())
        
    @staticmethod
    def exists(actorName,keyName):
        """
        Returns true if the actor.keyword table exists in the database

        In case the table exists but has not yet been accessed by the
        current server process, this method returns true.
        """
        tableName = KeyTable.name(actorName,keyName)
        return tableName in Table.existing or tableName in Table.registry

    @staticmethod
    def attach(actor,key):
        """
        Attaches a keyword's database table, creating it if necessary.
        """
        # construct this table's canonical name
        tableName = KeyTable.name(actor.name,key.name)
        # construct a tuple of column types for this keyword's value, prepended
        # by a raw_id column that links each row back to a timestamp and a
        # raw message string
        if not hasattr(key,'columnTypes'):
            colTypes = [ types.Long(name='raw_id') ]
            colTypes.extend(key.typedValues.vtypes)
            # cache the tuple in the key object to speed up future attach operations
            key.columnTypes = tuple(colTypes)
        # attach the table now
        keyTable = Table.attach(tableName,key.columnTypes,bufferSize=KeyTable.bufferSize,
            tableClass=KeyTable)
        keyTable.tag = '%s.%s' % (actor.name,key.name)
        # construct this table's SQL select statement preamble
        rawTable = Table.attach('reply_raw')
        keyTable.selector = 'select raw.%s' % rawTable.columnNames[1]
        for colName in keyTable.columnNames[1:]:
            keyTable.selector += ',key.%s' % colName
        keyTable.selector += ' from %s raw, %s key' % (rawTable.name,keyTable.name)
        keyTable.selector += ' where raw.%s=key.%s' % (
            rawTable.columnNames[0],keyTable.columnNames[0])
        keyTable.selectLimit = ' order by key.%s desc limit %%d;' % keyTable.columnNames[0]
        keyTable.noDuplicates = ' and key.%s < %%ld' % keyTable.columnNames[0]
        keyTable.selectAfter = ' and raw.%s > %%r' % rawTable.columnNames[1]
        keyTable.selectBefore = ' and raw.%s <= %%r' % rawTable.columnNames[1]
        return keyTable
        
    def byDate(self,interval,endAt):
        """
        Returns rows timestamped within the specified date range
        """
        # convert endAt from TAI seconds since the unix epoch into MJD secs
        if endAt == 'now':
            timestamp = astrotime.AstroTime.now(tz=KeyTable.timestampTZ)
        else:
            timestamp = astrotime.AstroTime.utcfromtimestamp(endAt)
        endAtMJDsecs = timestamp.MJD()*86400.
        beginMJDsecs = endAtMJDsecs - interval
        # retrieve any cached rows that match this query
        cacheCopy = [ ]
        if len(self.rowBuffer) > 0:
            # lookup the timestamp (MJD secs) of the oldest cached row
            cacheAge = self.taiCache[self.rowBuffer[0][0]].MJD()*86400.
            if cacheAge <= endAtMJDsecs:
                # the cached rows overlap the query range
                # copy matching rows, working from most recent to oldest
                for cachedRow in self.rowBuffer[::-1]:
                    rowMJDsecs = self.taiCache[cachedRow[0]].MJD()*86400.
                    if rowMJDsecs < beginMJDsecs:
                        break
                    if rowMJDsecs <= endAtMJDsecs:
                        cacheCopy.append(list(cachedRow))
                        # replace rawIDs with timestamps
                        cacheCopy[-1][0] = self.taiCache[cacheCopy[-1][0]]
            if cacheAge < beginMJDsecs:
                # the cached rows fully cover the query range so no database query is needed
                return defer.succeed(cacheCopy)
        # use the database to complete this query
        sql = self.selector
        # avoid duplicates in case the cache is flushed before our db query runs
        if len(self.rowBuffer) > 0:
            sql += self.noDuplicates % self.rowBuffer[0][0]
        sql += self.selectAfter % beginMJDsecs
        if endAt != 'now':
            sql += self.selectBefore % endAtMJDsecs
        # return rows ordered with most recent first and limit to 1000 (including the cache)
        sql += self.selectLimit % (1000-len(cacheCopy))
        return Table.connectionPool.runInteraction(
            keyTableFetch,sql,self.columnFinalTypes,cacheCopy)

    def recent(self,nRows):
        """
        Returns the most recent rows added to this key table as a Deferred
        """
        # copy (and reverse) any recent rows currently cached in memory
        cacheCopy = [ ]
        for cachedRow in self.rowBuffer[-1:-1-nRows:-1]:
            cacheCopy.append(list(cachedRow))
            # replace rawIDs with timestamps
            cacheCopy[-1][0] = self.taiCache[cacheCopy[-1][0]]
        # does the cache contain all the recent rows requested?
        if len(cacheCopy) == nRows or not Table.connectionPool:
            return defer.succeed(cacheCopy)
        # we still need to retrieve some rows from the database
        sql = self.selector
        # avoid duplicates in case the cache is flushed before our db query runs
        if len(self.rowBuffer) > 0:
            sql += self.noDuplicates % self.rowBuffer[0][0]
        sql += self.selectLimit % (nRows-len(cacheCopy))
        return Table.connectionPool.runInteraction(
            keyTableFetch,sql,self.columnFinalTypes,cacheCopy)
    
    def openBuffer(self):
        """
        Creates an empty rawID-TAI map
        """
        self.taiCache = { }
        return Table.openBuffer(self)
        
    def record(self,tai,rawID,*rowValues):
        """
        Remembers the timestamp associated with a raw ID
        """
        self.taiCache[rawID] = tai
        return Table.record(self,rawID,*rowValues)
    
