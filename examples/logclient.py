#!/usr/bin/env python

import re
import socket
import fileinput
import time

import archiver.protocol
EOL = archiver.protocol.MessageReceiver.delimiter

def logfeed(instream,outstream,options):
    # regexp for a TCC reply as it appears in the telrun log
    fields = re.compile('(0|[1-9][0-9]*) (0|[1-9][0-9]*) ([>iIwW:fF!])(.*)')
    totalCount = 0
    maxCount = options.burst*options.repeat
    # convert the burst interval from ms to secs
    options.ival = 1e-3*options.ival
    for line in instream:
        # does the text following the timestamp look like a TCC reply?
        matched = fields.match(line[25:])
        if matched:
            # starting a new burst?
            if totalCount % options.burst == 0:
                print 'burst start'
                burstStart = time.time()
                if options.trace:
                    print >>options.traceFile, "%d %f" % (
                        totalCount,burstStart-options.traceStart)
            # reformat the message to look like it came via the hub
            (commandId,userNum,replyCode,replyText) = matched.groups()
            msg = 'telrun.user%s %s %s %s%s%s' % (
                userNum,commandId,options.actor,replyCode,replyText,EOL)
            outstream.sendall(msg)
            totalCount += 1
            # reached the end of a message burst?
            if totalCount % options.burst == 0:
                print 'burst stop'
                if options.trace:
                    print >>options.traceFile, "%d %f" % (
                        totalCount,time.time()-options.traceStart)
                burstDuration = time.time() - burstStart
                remaining = options.ival - burstDuration
                if remaining > 0:
                    time.sleep(remaining)
                else:
                    print 'burst interval too short: %f sec' % options.ival
            # reached the total number of bursts requested?
            if totalCount >= maxCount:
                break
    print 'fed %d lines' % totalCount


if __name__ == '__main__':
    
    # configure the command-line options processing
    from opscore.utility.config import ConfigOptionParser
    cli = ConfigOptionParser(
        usage='usage: %prog [options] [msg-stream]',config_section='logclient')
    cli.add_option(
        '--ival',type='int',action='store',help='burst repetition INTERVAL (ms)',
        metavar='INTERVAL'
    )
    cli.add_option(
        '--burst',type='int',action='store',help='NUMBER of messages per burst',
        metavar='NUMBER'
    )
    cli.add_option(
        '--repeat',type='int',action='store',help='total NUMBER of bursts to send',
        metavar='NUMBER'
    )
    cli.add_option(
        '--trans',help='network TRANSPORT type to use',
        choices=['inet','unix'],metavar='TRANSPORT'
    )
    cli.add_option(
        '--trace',help='FILE to store trace timings',metavar='FILE'
    )
    cli.add_option(
        '--actor',help='format messages as if they come from ACTOR',default='tcc'
    )
    (options,args) = cli.parse_args()

    # open a trace file, if requested
    if options.trace:
        options.traceFile = open(options.trace,'w')
        options.traceStart = time.time()
        print >>options.traceFile, 'START %f' % options.traceStart

    # open a socket using the requested transport
    if options.trans == 'inet':
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost',1966))
    elif options.trans == 'unix':
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/tmp/archiver.sock')
    else:
        raise RuntimeError('Invalid transport type: %s' % options.trans)
        
    # feed messages
    logfeed(fileinput.input(args),s,options)

    s.close()
    if options.trace:
        options.traceFile.close()
