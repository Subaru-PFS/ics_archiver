"""
Archiver support for tracking SDSS-3 hub actors
"""
from __future__ import print_function
from __future__ import absolute_import

# Created 06-Mar-2009 by David Kirkby (dkirkby@uci.edu)

from twisted.python import log

from opscore.protocols import keys,types
from . import database

class ActorException(Exception):
    pass

class Actor(object):
    
    existing = { }
    registry = { }

    # the SQL table name containing known actors (should be lowercase)
    tableName = 'actors'
    table = None
    
    @staticmethod
    def attach(name,dictionaryRequired=False):
        if name in Actor.registry:
            actor = Actor.registry[name]
            if dictionaryRequired and not actor.kdict:
                return None
            else:
                return actor
        else:
            return Actor(name,dictionaryRequired)
    
    def __init__(self,name,dictionaryRequired):
        self.name = name
        # first time initialization of database table
        if not Actor.table:
            Actor.table = database.Table.attach(Actor.tableName,(
                types.UInt(name='id'),
                types.String(name='name'),
                types.Int(name='major'),
                types.Int(name='minor'),
                types.String(name='checksum')
            ),bufferSize=3)
        # try to (re)load this actor's dictionary
        try:
            log.msg('loading keydict for %s' % (name))
            self.kdict = keys.KeysDictionary.load(name,forceReload=True)
            cksum = self.kdict.checksum
            (major,minor) = self.kdict.version
        except keys.KeysDictionaryError as e:
            if dictionaryRequired:
                raise ActorException('No %s dictionary available' % name)
            log.err('dictionary load error: %s' % e)
            self.kdict = None
            cksum = ''
            (major,minor) = (0,0)
            
        # is this actor already in the database?
        if name in Actor.existing:
            (ex_idnum,ex_major,ex_minor,ex_cksum) = Actor.existing[name]
            if (major,minor) == (ex_major,ex_minor):
                if cksum != ex_cksum:
                    raise ActorException(
                        'Dictionary has changed without version update for %s %d.%d' %
                        (name,major,minor))
                print('re-initializing %s actor version %d.%d' % (name,major,minor))
                self.idnum = ex_idnum
            elif major < ex_major or (major == ex_major and minor < ex_minor):
                raise ActorException(
                    'Found old dictionary for %s? %d.%d < %d.%d' %
                    (name,major,minor,ex_major,ex_minor))
            else:
                print('updating %s actor from %d.%d to %d.%d' % (
                    name,ex_major,ex_minor,major,minor))
                self.create(major,minor,cksum)
        else:
            print('recording new %s actor in database (version %d.%d)' % (name,major,minor))
            self.create(major,minor,cksum)
            
        # initialize our keyword statistics
        self.keyStats = { }
        
        # remember this actor for this session
        Actor.registry[self.name] = self

    def create(self,major,minor,cksum):
        self.idnum = Actor.table.nRows
        Actor.table.record(self.idnum,self.name,major,minor,cksum)
        
    @staticmethod
    def allNames():
        """
        Returns an alphabetical list of all known actor names
        """
        return sorted(set(Actor.existing.keys()).union(Actor.registry.keys()))
