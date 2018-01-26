"""
Twisted protocols to handle SDSS-3 operations messages
"""
from __future__ import print_function
from __future__ import absolute_import

# Created 27-Feb-2009 by David Kirkby (dkirkby@uci.edu)

from builtins import str
from datetime import datetime

import twisted.internet.error
from twisted.python import log
from twisted.protocols.basic import LineOnlyReceiver as Receiver

from opscore.protocols import parser,types,keys,validation
from opscore.utility import astrotime
from . import database,actors,monitor

class MessageReceiver(Receiver):
    
    delimiter = '\n' # lines ending with \r\n (e.g., from telnet) will have trailing \r
    
    def __init__(self,name=None):
        self.name = name
        if not self.name:
            self.name = self.__class__.__name__
        self.messagesReceived = 0
        self.bytesReceived = 0
        # disable compound value wrapping (eg, PVTs)
        types.CompoundValueType.WrapEnable = False
    
    def connectionMade(self):
        self.connectedSince = datetime.now()
        print('%s: connected to %s' % (self.name,self.transport.getPeer()))
        return Receiver.connectionMade(self)

    def connectionLost(self,reason):
        print(self)
        if reason.check(twisted.internet.error.ConnectionDone):
            print('%s: connection closed.' % self.name)
        else:
            print('%s: connection lost: %s' % (self.name,reason))
        self.connectedSince = None
        return Receiver.connectionLost(self,reason)

    def lineReceived(self, message):
        self.messagesReceived += 1
        # strip off a trailing \r (this allows us to accept lines via telnet)
        if message[-1] == '\r':
            message = message[:-1]
        self.bytesReceived += len(message)
        self.messageReceived(message)
        
    def lineLengthExceeded(self,message):
        print('%s: max line length exceeded: %d > %d' % (
            self.name,len(message),self.MAX_LENGTH
        ))
        return Receiver.lineLengthExceeded(self,message)

    def __str__(self):
        if self.connectedSince:
            uptime = datetime.now() - self.connectedSince
            return '%s: recieved %d messages (%d bytes) from %s (connected %s)' % (
                self.name,self.messagesReceived,self.bytesReceived,
                self.transport.getPeer(),uptime
            )
        else:
            return '%s: not connected'

class CommandReceiver(MessageReceiver):
    
    def __init__(self):
        MessageReceiver.__init__(self)
        # initialize a command message parser
        self.cmdParser = parser.CommandParser()
        # define our command keywords
        self.kdict = keys.KeysDictionary(
            '<cmd>',(0,1),
            keys.Key('name',types.String(help='name of expression')),
            keys.Key('expr',types.String()),
            keys.Key('help',types.String(help='description of expression')),
            keys.Key('timeout',types.UInt(units='s',help='expiration timeout')),
            keys.Key('history',types.UInt(units='s',help='amount of history to preload')),
            keys.Key('id',types.String(help='Subscriber ID')),
        )
        keys.CmdKey.setKeys(self.kdict)
        # define our command set
        self.commandSet = (
            validation.Cmd('monitor','info') >> self.monitorInfo,
            validation.Cmd('monitor','create <name> <expr> [<help>]'
                ) >> self.monitorCreate,
            validation.Cmd('monitor','drop <name>') >> self.monitorDrop,
            validation.Cmd('subscribe','<name> [<timeout>] [<history>]'
                ) >> self.monitorSubscribe,
            validation.Cmd('flush','<id>') >> self.monitorFlush,
        )

    def monitorFlush(self,cmd):
        subid = cmd.keywords['id'].values[0]
        try:
            update = monitor.flush(subid)
            for row in update:
                self.sendLine(repr(row))
            self.sendLine('Flush contained %d row(s)' % len(update))
        except monitor.MonitorError as e:
            self.sendLine(str(e))
    
    def monitorSubscribe(self,cmd):
        name = cmd.keywords['name'].values[0]
        kwargs = { }
        if 'timeout' in cmd.keywords:
            kwargs['timeout'] = cmd.keywords['timeout'].values[0]
        if 'history' in cmd.keywords:
            kwargs['history'] = cmd.keywords['history'].values[0]
        log.msg('Subscribing to %s' % name)
        try:
            subid = monitor.subscribe(name,**kwargs)
            self.sendLine('Created subscriber id %s' % subid)
        except monitor.MonitorError as e:
            self.sendLine(str(e))
    
    def monitorInfo(self,cmd):
        log.msg('Reporting monitoring info')
        info = monitor.lineInfo()
        for name,expr,help,nsub in info:
            self.sendLine('%s = %s' % (name,expr))
            self.sendLine('  Subscribers: %d' % nsub)
            if help:
                self.sendLine('  Description: %s' % help)
        self.sendLine('Monitoring %d expression(s)' % len(info))
        info = monitor.subscriberInfo()
        for subid,name,timeout,expired in info:
            self.sendLine(
                'Subscriber %s follows %s with timeout %d (last flush %.0fs ago)' %
                (subid,name,timeout,expired))
        self.sendLine('Current subscribers: %d' % len(info))
    
    def monitorCreate(self,cmd):
        name = cmd.keywords['name'].values[0]
        expr = cmd.keywords['expr'].values[0]
        help = cmd.keywords['help'].values[0] if 'help' in cmd.keywords else None
        log.msg('Creating expression "%s" as %s' % (name,expr))
        try:
            monitor.create(name,expr,help)
        except monitor.MonitorError as e:
            self.sendLine(str(e))
    
    def monitorDrop(self,cmd):
        name = cmd.keywords['name'].values[0]
        log.msg('Dropping monitor %s' % name)
        try:
            monitor.drop(name)
        except monitor.MonitorError as e:
            self.sendLine(str(e))
    
    def messageReceived(self,message):
        # try to parse this message as a command
        try:
            handled = False
            parsed = self.cmdParser.parse(message)
            for cmd in self.commandSet:
                handled = cmd.consume(parsed)
                if handled:
                    break
            if handled:
                self.sendLine('ok')
            else:
                self.sendLine('unknown command')
        except parser.ParseError as e:
            log.err('%s: unable to parse message: %s' % (self.name,e))
            self.sendLine('Parse error: %s' % e)

class ReplyReceiver(MessageReceiver):

    def __init__(self):
        MessageReceiver.__init__(self)
        # initialize a reply message parser
        self.replyParser = parser.ReplyParser()
        # lookup the core reply tables we will fill
        self.replyRaw = database.Table.attach('reply_raw')
        self.replyHdr = database.Table.attach('reply_hdr')
        # use UTC for timestamps when the system clock is actually tracking TAI
        if MessageReceiver.options.systemClock == 'TAI':
            self.timestampTZ = astrotime.UTC
        else:
            self.timestampTZ = astrotime.TAI
        
    def messageReceived(self,message):
        # timestamp this message in TAI MJD seconds
        now = astrotime.AstroTime.now(tz=self.timestampTZ)
        tai = now.MJD()*86400.
        # record the raw reply message before trying to interpret it
        rawID = self.replyRaw.nRows
        self.replyRaw.record(rawID,tai,message)
        # try to parse the reply message
        try:
            parsed = self.replyParser.parse(message)
            # lookup this actor
            hdr = parsed.header
            actor = actors.Actor.attach(hdr.actor)
            actorID = actor.idnum
            # loop over this message's keywords if we have a dictionary available
            if not actor.kdict:
                keyErrors = len(parsed.keywords)
            else:
                keyErrors = 0
                for keyword in parsed.keywords:
                    keytag = '%s.%s' % (actor.name.lower(),keyword.name.lower())
                    # look up this keyword's validator in the actor's dictionary
                    try:
                        key = actor.kdict[keyword.name]
                        if key.consume(keyword):
                            # write this keyword to its own table
                            try:
                                keyTable = database.KeyTable.attach(actor,key)
                                keyTable.record(now,rawID,*tuple(keyword.values))
                                # update actor key statistics
                                actor.keyStats[keyword.name] = (
                                    actor.keyStats.get(keyword.name,0) + 1)
                            except Exception as e:
                                log.err('Error writing to %s: %s (see below)'
                                    % (keytag,e.__class__.__name__))
                                log.err(str(e))
                                keyErrors += 1
                        else:
                            log.err('Invalid keyword values for %s' % keytag)
                            keyErrors += 1
                    except KeyError:
                        log.err('Unknown keyword %s' % keytag)
                        keyErrors += 1
            # record the reply header fields
            self.replyHdr.record(rawID,actorID,hdr.program,hdr.user,
                hdr.commandId,hdr.code,keyErrors)
        except parser.ParseError as e:
            log.err('%s: unable to parse message: %s' % (self.name,e))
        except actors.ActorException as e:
            log.err('%s: unable to attach actor: %s' % (self.name,e))
