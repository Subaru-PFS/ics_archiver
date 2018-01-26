#!/usr/bin/env python
"""
Archiver server program

Refer to https://trac.sdss3.org/wiki/Ops/Arch/Server for details.
"""
from __future__ import print_function

# Created 27-Feb-2009 by David Kirkby (dkirkby@uci.edu)

from past.builtins import basestring
from builtins import object
import sys
import time
import os,os.path
import getpass
import socket

import archiver.protocol
import archiver.database
import archiver.web
from archiver.utils import getEnvPath, LevelFileLogObserver
import logging
from twisted.python.logfile import LogFile

class ServerInfo(object):
    pass

def startServer(options):
    """
    Configures and starts the twisted event loop to provide archiver services
    """
    from twisted.python import log
    from twisted.internet import protocol,reactor,task

    # record some info about how the server was started
    info = ServerInfo()
    info.startedAt = time.time()
    info.pid = os.getpid()
    info.user = getpass.getuser()
    info.host = socket.getfqdn()
    info.commandLine = ' '.join(sys.argv)

    # create a unique temporary path to hold our logging and buffers
    options.tmpPath = getEnvPath(options.tmpPath)
    options.listenPath = getEnvPath(options.listenPath)
    options.cmdPath = getEnvPath(options.cmdPath)

    print('Running',__file__,'as PID',info.pid)
    if 'PID' in options.tmpPath:
        options.tmpPath = options.tmpPath.replace('PID','%d') % info.pid
    assert not os.path.exists(options.tmpPath)
    os.makedirs(options.tmpPath)
    print('Starting archive server with output to',options.tmpPath)

    # start logging all stdout and stderr traffic
    if options.interactive:
        log.startLogging(sys.stdout)
    else:
        f = LogFile("server.log", options.tmpPath+'/', rotateLength=1000000000)
        logger = LevelFileLogObserver(f, logging.DEBUG)
        log.addObserver(logger.emit)

    # find our product dir
    info.productDir = os.getenv('ICS_ARCHIVER_DIR')
    if not info.productDir:
        print('ICS_ARCHIVER_DIR is not defined. Will try to use working dir.')
        info.productDir = os.getcwd()

    # startup the database
    archiver.protocol.MessageReceiver.options = options
    archiver.database.init(options)
    
    # define a periodic timer interrupt handler
    def ping():
        archiver.database.ping(options)
    pinger = task.LoopingCall(ping)
    if options.pingInterval > 0:
        pinger.start(options.pingInterval)

    # configure and start the reactor event loop
    try:
        # listen for clients sending reply messages
        replyFactory = protocol.Factory()
        replyFactory.protocol = archiver.protocol.ReplyReceiver
        if options.listenPort > 0:
            print('Listening for replies on TCP port %d' % options.listenPort)
            reactor.listenTCP(options.listenPort,replyFactory)
        if options.listenPath:
            print('Listening for replies on UNIX path %s' % options.listenPath)
            reactor.listenUNIX(options.listenPath,replyFactory)
        
        # connect to the hub's reply message stream        
        if options.hubHost and options.hubPort > 0:
            print('Looking for the hub at %s:%d' % (options.hubHost,options.hubPort))
            class HubFactory(protocol.ReconnectingClientFactory):
                initialDelay = options.hubInitialDelay
                factor = options.hubDelayFactor
                maxDelay = options.hubMaxDelay*3600.0
                def buildProtocol(self,addr):
                    print('Connected to the hub')
                    handler = archiver.protocol.ReplyReceiver()
                    handler.factory = self
                    self.resetDelay()
                    return handler
            reactor.connectTCP(options.hubHost,options.hubPort,HubFactory())
            
        # listen for clients sending command messages
        cmdFactory = protocol.Factory()
        cmdFactory.protocol = archiver.protocol.CommandReceiver
        if options.cmdPort > 0:
            print('Listening for commands on TCP port %d' % options.cmdPort)
            reactor.listenTCP(options.cmdPort,cmdFactory)
        if options.cmdPath:
            print('Listening for commands on UNIX path %s' % options.cmdPath)
            reactor.listenUNIX(options.cmdPath,cmdFactory)
           
        # start up a web server
        if options.httpPort > 0:
            reactor.listenTCP(options.httpPort,
                archiver.web.ArchiverSite(info,options))
        
        reactor.run()
    
    except Exception:
        print('Reactor started failed')
        # need to release unix socket cleanly here...
        if options.listenPath:
            os.unlink(options.listenPath)
        raise


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if isinstance(argv, basestring):
        import shlex
        argv = shlex.split(argv)
        
    import opscore.utility.config as config
    cli = config.ConfigOptionParser(
        product_name='ics_archiver',config_file='archiver.ini',config_section='server'
    )
    
    cli.add_option('-i','--interactive',action='store_true',default=False,
        help='running interactively?')
    cli.add_option('--tmp-path',dest='tmpPath',default='archiver-PID',
        help='temporary path for server log and buffer files')
    cli.add_option('--db-engine',dest='dbEngine',choices=('postgres','mysql','none'),
        help='database engine to use')
    cli.add_option('--ping-interval',dest='pingInterval',type='float',
        help='interval in seconds to run periodic ping task at')
    cli.add_option('--listen-port',dest='listenPort',type='int',default=0,
        help='TCP port number to listen for replies on or zero for none')
    cli.add_option('--listen-path',dest='listenPath',default='',
        help='UNIX socket path to listen for replies on empty string for none')
    cli.add_option('--cmd-port',dest='cmdPort',type='int',default=0,
        help='TCP port number to listen for commands on or zero for none')
    cli.add_option('--cmd-path',dest='cmdPath',default='',
        help='UNIX socket path to listen for commands on or empty string for none')
    cli.add_option('--hub-host',dest='hubHost',default='',
        help='Hostname of operations software hub')
    cli.add_option('--hub-port',dest='hubPort',type='int',default=0,
        help='Port number for hub connection or zero for no connection')
    cli.add_option('--http-port',dest='httpPort',type='int',default=0,
        help='Port number of web server or zero to disable server')
    cli.add_option('--db-host',dest='dbHost',
        help='Hostname of database server')
    cli.add_option('--db-user',dest='dbUser',type='string',
        help='Username for database transactions')
    cli.add_option('--db-password',dest='dbPassword',type='string',
        help='Password for database transactions')
    cli.add_option('--db-name',dest='dbName',
        help='Name of database containing archiver tables')
    cli.add_option('--raw-buffer-size',dest='rawBufferSize',type='int',default=10,
        help='Buffer size for raw reply message database table')
    cli.add_option('--hdr-buffer-size',dest='hdrBufferSize',type='int',default=10,
        help='Buffer size for reply message header database table')
    cli.add_option('--key-buffer-size',dest='keyBufferSize',type='int',default=10,
        help='Buffer size for reply keyword database tables')
    cli.add_option('--trace-list',dest='traceList',default='',
        help='comma-separated list of tables for activity tracing')
    cli.add_option('--idle-time',dest='idleTime',type='float',
        help='flush tables after no activity for IDLETIME (seconds)')
    cli.add_option('--hub-initial-delay',dest='hubInitialDelay',type='float',
        help='initial delay before attempting to reconnect to hub (seconds)')
    cli.add_option('--hub-delay-factor',dest='hubDelayFactor',type='float',
        help='factor to increase delay by for subsequenct hub reconnection attempts')
    cli.add_option('--hub-max-delay',dest='hubMaxDelay',type='float',
        help='maximum hub reconnect delay before giving up (hours)')
    cli.add_option('--system-clock',dest='systemClock',choices=('UTC','TAI'),
        help='Does system clock track UTC or TAI?')
        
    (options,args) = cli.parse_args(argv)

    startServer(options)

if __name__ == '__main__':
    main()

