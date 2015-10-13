#!/usr/bin/env python
"""
Simulates the hub
"""

from twisted.internet import protocol,reactor,task
from twisted.protocols import basic

hubPort = 6095

fakeData = """\
.hub 0 hub i user="APO.Craig","TUI","2009-02-05 1.6a6","Darwin-9.6.0-i386-32bit aqua","140.180.0.79","dynamic-oit-vapornet-a-61.Princeton.EDU"
.hub 0 hub i Commanders="client_1","APO.Craig","nclient_31"
.hub 0 hub i users="APO.Craig"
.tcc 0 tcc I Modu="exe_BrdTelPos"; Text="1613400 packets sent successfully"
.mcp 0 mcp i aliveAt=1240512177"""

class HubFeed(basic.LineOnlyReceiver):    
    def connectionMade(self):
        for line in fakeData.split('\n'):
            self.sendLine(line.strip())

def start():
    factory = protocol.Factory()
    factory.protocol = HubFeed
    reactor.listenTCP(hubPort,factory)
    print 'hub simulator ready on port',hubPort
    reactor.run()
    
if __name__ == '__main__':
    start()
