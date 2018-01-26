"""
Archiver keyword monitoring interface

Refer to https://trac.sdss3.org/wiki/Ops/Arch/Server for details.
"""
from __future__ import print_function

# Created 03-Aug-2009 by David Kirkby (dkirkby@uci.edu)

from archiver import expression,database,actors
from opscore.protocols import keys

from twisted.internet import defer

import time

class MonitorError(Exception):
    pass

lines = { }
subscriptions = { }
subscribers = { }

def create(name,expr,help=None):
    """
    Creates a new keyword expression to monitor
    """
    lcname = name.lower()
    if lcname in lines:
        raise MonitorError('Name already in use: %s' % name)
    lines[lcname] = MonitorExpression(name,expr,help)
    subscriptions[lcname] = [ ]
    
def lineInfo():
    """
    Returns info about keyword expressions being monitored
    """
    info = [ ]
    # reconstruct the original name cases
    for name in sorted(lines.keys()):
        line = lines[name]
        info.append((line.name,line.expr,line.help,len(subscriptions[name])))
    return info
    
def subscriberInfo():
    """
    Returns info about monitoring subscribers
    """
    info = [ ]
    now = time.time()
    for subid,sub in subscribers.iteritems():
        info.append((subid,sub.monitorExpr.name,sub.timeout,now-sub.lastFlush))
    return info
    
def drop(name):
    """
    Drops the named expression
    """
    lcname = name.lower()
    if not lcname in lines:
        raise MonitorError('No such monitor: %s' % name)
    if subscriptions[lcname]:
        raise MonitorError('Cannot drop monitor with subscribers')        
    del lines[lcname]
    
def subscribe(name,timeout=None,history=None):
    """
    Subscribes to the named expression
    """
    lcname = name.lower()
    if not lcname in lines:
        raise MonitorError('No such monitor to subscribe to: %s' % name)
    # create a new subscription with a 1-hr default timeout
    sub = MonitorSubscription(lines[lcname],timeout or 3600,history)
    # record this subscription
    subscriptions[lcname].append(sub)
    # generate and return a unique subscriber id
    subscribers[sub.id] = sub
    return sub.id
    
def flush(subid):
    """
    Returns any pending data for a subscription
    """
    try:
        sub = subscribers[subid]
        return sub.flush()
    except KeyError:
        raise MonitorError('No such subscriber with ID %s' % subid)
    
class MonitorSubscription(object):
    """
    Represents a limited time subscription to a monitored expression
    """
    def __init__(self,monitorExpr,timeout,history):
        self.monitorExpr = monitorExpr
        self.timeout = timeout
        self.buffer = [ ]
        self.lastFlush = time.time()
        self.id = '%08x' % id(self)
        if history:
            self.waiting = True
            self.monitorExpr.loadByDate(history,'now').addCallback(self.gotHistory)
        
    def gotHistory(self,history):
        print('=== gotHistory','='*20)
        print(repr(history))
        for (keytag,timestamp,values) in history:
            self.update(keytag,timestamp,values)
        self.waiting = False

    def update(self,keytag,timestamp,values):
        """
        Updates this subscription to reflect new keyword values
        """
        if time.time() - self.lastFlush < self.timeout:
            if self.monitorExpr.update(keytag,values):
                value = self.monitorExpr.value()
                if value is not None:
                    self.buffer.append((timestamp,value))
        else:
            # subscription has expired: no more updates will be accepted
            lcname = sef.monitorExpr.name.lower()
            sublist = subscriptions[lcname].remove(self)
            del subscribers[sub.id]
            log.msg('Expired subscription ID %s' % sub.id)

    def flush(self):
        """
        Returns the new data since the last update
        """
        self.lastFlush = time.time()
        update = self.buffer
        self.buffer = [ ]
        return update

class MonitorExpression(object):
    """
    Represents a single monitored keyword expression
    """
    parser = expression.Parser()
    
    def __init__(self,name,expr,help=None):
        self.name = name
        self.expr = expr
        self.help = help
        self.tables = [ ]
        self.parsed = self.parser.parse(expr)
        self.register(self.parsed)
    
    def register(self,node):
        # perform a depth-first traversal of the epxression tree
        for child in node.children:
            self.register(child)
        if not isinstance(node,expression.KeyValue):
            return
        print('Registering',node)
        # is this a valid actor?
        actorName = node.args[0].lower()
        try:
            actor = actors.Actor.attach(actorName,dictionaryRequired=True)
        except actors.ActorException:
            raise MonitorError('Invalid actor in %s' % node)
        # is this a valid keyword?
        keyName = node.args[1].lower()
        if not keyName in actor.kdict:
            raise MonitorError('Invalid keyword in %s' % node)
        # attach this keyword's database table
        try:
            key = actor.kdict[keyName]
            table = database.KeyTable.attach(actor,key)
            self.tables.append(table)
        except Exception as e:
            raise MonitorError('Unable to attach %s.%s:\n%s' % (actorName,keyName,str(e)))
        # is this a valid value name?
        foundIndex = None
        if node.valueItem is None:
            # by default, use the first value (if possible)
            if len(table.aliases) < 2:
                raise MonitorError('Keyword %s.%s has no values' % (actorName,keyName))
            foundIndex = 0
        else:
            for index,alias in enumerate(table.aliases):
                if node.valueItem == alias:
                    # offset by one for raw_id
                    foundIndex = index-1
                    break
        if foundIndex is None or foundIndex < 0:
            raise MonitorError('Invalid keyword value in %s' % node)
        node.valueItem = foundIndex
        
    def mergeTables(self,results):
        updates = { }
        for (success,tableData),table in zip(results,self.tables):
            if not success:
                raise MonitorError('Unable to load data for %s' % table.tag)
            print('processing %d rows from %s' % (len(tableData),table.tag))
            for row in tableData:
                timestamp,values = row[0],row[1:]
                tai = timestamp.MJD()*86400.
                updates[tai] = (table.tag,tai,values)
        # merge the updates with a master sort on TAI
        return [ updates[t] for t in sorted(updates) ]

    def loadByDate(self,interval,endAt):
        defers = [ ]
        for table in self.tables:
            defers.append(table.byDate(interval,endAt))
        return defer.DeferredList(defers).addCallback(self.mergeTables)
        
    def value(self):
        return self.parsed.value
        
    def update(self,keytag,values):
        # could implement cacheing here to optimize multiple suscribers
        return self.parsed.update(keytag,values)
