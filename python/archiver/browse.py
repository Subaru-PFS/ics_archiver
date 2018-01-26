"""
Archiver web browser interface

Refer to https://trac.sdss3.org/wiki/Ops/Arch/Server for details.
"""
from __future__ import print_function

# Created 22-Jun-2009 by David Kirkby (dkirkby@uci.edu)

import datetime

from archiver import actors,web,database
from opscore.utility import html
from opscore.protocols import types

def appendToHTMLTable(tableData,table):
    """
    Returns the input HTML table with tableData appended
    """
    for rowData in tableData:
        row = html.Tr()
        for cellData in rowData:
            if isinstance(cellData,datetime.datetime):
                # use ISO format but drop the time zone offset
                row.append(html.Td(cellData.isoformat()[:-6]))
            elif cellData is types.InvalidValue:
                row.append(html.Td(className='invalid',title='Invalid value'))
            else:
                row.append(html.Td(str(cellData)))
        row['className'] = 'datarow'
        table.append(row)
    return table

class BrowseHandler(web.WebQuery):
    
    title = 'SDSS3 Archiver Browser'
    
    ivalMultipliers = { 's': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800 }
    
    def __init__(self,serviceName,options):
        self.options = options
        web.WebQuery.__init__(self,serviceName)

    def GET(self,request,session,state,content):
        # use our javascript library
        content.root.head.js.append('/static/browse.js')
        # render the text inputs for actor.keyword
        inputs = html.Div(
            html.Input(value='actor',type='text',id='actor'),'.',
            html.Input(value='keyword',type='text',id='keyword'),
            id='inputs'
        )
        # display a message area
        choiceHead = html.Div('Javascript must be enabled to use this page',id='messages')
        # display a list of actor choices
        actorChoices = html.Div(id='actor-choices')
        for actorName in actors.Actor.allNames():
            # only generate a link for actors with a dictionary available
            actor = None
            try:
                actor = actors.Actor.attach(actorName.lower(),dictionaryRequired=True)
            except actors.ActorException as e:
                print(str(e))
            if actor:
                node = html.A(actorName,href='?actor=%s'%actorName,
                    title='Using %s dictionary version %d.%d' %
                    (actorName,actor.kdict.version[0],actor.kdict.version[1]))
            else:
                node = html.Span(actorName,
                    title='No dictionary available for the %s actor' % actorName)
            # separate choices by a (breaking) space
            actorChoices.extend([node,' '])
        choiceFoot = html.Div(html.A('',href='#',title='A direct link to this view'),
            className='footer')
        choices = html.Div(choiceHead,actorChoices,choiceFoot,id='choices')
        # append an empty browser div that will display browsed data
        browser = html.Div(
            html.Ul(
                html.Li(html.A(html.Span('Recent'),href='#recent')),
                html.Li(html.A(html.Span('By Date'),href='#bydate')),
                html.Li(html.A(html.Span('Documentation'),href='#doc'))
            ),
            html.Div(
                html.Div('Displaying the ',
                    html.Input(id='nrecent',type='text',className='med-text'),
                    ' most recent updates.',
                    html.Button('UPDATE',type='button',id='recent-update'),
                    id='recent-msg'),
                html.Div('',id='recent-content'),
                html.Div(
                    html.Div('No data received yet'),
                    html.Div(html.A('',href='#',title='A direct link to this view')),
                    className='footer'),
                id='recent'),
            html.Div(
                html.Div(
                    'Displaying ',
                    html.Input(id='nduration',type='text',className='med-text'),
                    html.Select(
                        html.Option('second(s)',value='s'),
                        html.Option('minute(s)',value='m'),
                        html.Option('hour(s)',value='h',selected='yes'),
                        html.Option('day(s)',value='d'),
                        html.Option('week(s)',value='w'),
                        id='duration-unit'),
                    ' ending ',
                    html.Select(
                        html.Option('now',value='now',selected='yes'),
                        html.Option('on...',value='on'),
                        id='on-type'),
                    html.Span(
                        html.Input(type='text',id='from-date',className='long-text'),
                        ' at TAI ',
                        html.Input(type='text',id='from-hour',className='short-text'),
                        ':',
                        html.Input(type='text',id='from-min',className='short-text'),
                        ':',
                        html.Input(type='text',id='from-sec',className='short-text'),
                        id='on-input'),
                    html.Button('UPDATE',type='button',id='bydate-update'),
                    id='bydate-msg'),
                html.Div('',id='bydate-content'),
                html.Div(
                    html.Div('No data received yet'),
                    html.Div(html.A('',href='#',title='A direct link to this view')),
                    className='footer'),
                id='bydate'),
            html.Div(
                html.Div(html.A('',href='#',title='A direct link to this view'),
                    className='footer'),
                id='doc'),
            id='browser')
        # append an empty error container
        errors = html.Div(html.Span(''),id='errors')
        # center everything
        centered = html.Div(inputs,choices,browser,errors,className='centered')
        content.append(centered)

    def error(self,msg):
        return html.Div(msg,className='error')

    def POST(self,request,session,state):
        actorName = request.args.get('actor',[None])[-1]
        keyName = request.args.get('key',[None])[-1]
        nRecent = request.args.get('recent',[None])[-1]
        interval = request.args.get('ival',[None])[-1]
        endAt = request.args.get('end',[None])[-1]
        if actorName:
            if (actorName not in actors.Actor.existing and
                actorName not in actors.Actor.registry):
                return self.error('Uknown actor: %s' % actorName)
            else:
                try:
                    actor = actors.Actor.attach(actorName.lower(),dictionaryRequired=True)
                except actors.ActorException as e:
                    return self.error('Unable to load dictionary for %s' % actorName)
                if keyName:
                    if keyName.lower() not in actor.kdict:
                        return self.error('Unknown keyword %s.%s' % (actorName,keyName))
                    else:
                        key = actor.kdict[keyName.lower()]
                        # is this a documentation request?
                        if 'doc' in request.args:
                            return key.describeAsHTML()
                        # otherwise, the keyword must already have a db table
                        if not database.KeyTable.exists(actorName,keyName):
                            return self.error('No data recorded for %s.%s'
                                % (actorName,keyName))
                        try:
                            keyTable = database.KeyTable.attach(actor,key)
                        except Exception as e:
                            errmsg = ('Unable to read %s.%s: %s'
                                % (actorName,keyName,e.__class__.__name__))
                            return self.error(errmsg)
                            log.err(errmsg)
                            log.err(str(e))
                        if nRecent:
                            try:
                                nRecent = int(nRecent)
                            except ValueError:
                                return self.error("Invalid value for parameter 'recent'")
                        if interval:
                            unit = interval[-1]
                            duration = None
                            try:
                                duration = int(interval[:-1])
                            except ValueError:
                                pass
                            if unit not in self.ivalMultipliers or not duration:
                                return self.error("Invalid value for parameter 'ival'")
                            interval = duration*self.ivalMultipliers[unit]
                        if endAt:
                            if endAt != 'now':
                                try:
                                    endAt = int(endAt)
                                except ValueError:
                                    return self.error("Invalid value for parameter 'end'")
                        # if we get this far we have a valid query so prepare a table
                        table = html.Table()
                        # The first header row lists the column names and the first
                        # column is always a timestamp
                        hdr = html.Tr(html.Th('timestamp',title='When this row was recorded'))
                        for alias,vType in zip(
                            keyTable.aliases[1:],keyTable.columnFinalTypes[1:]):
                            hdr.append(html.Th(alias,
                                title=vType.help or 'No help available for this value'))
                        table.append(hdr)
                        # The second header row lists the column value units
                        hdr = html.Tr(html.Th('TAI'))
                        for vType in keyTable.columnFinalTypes[1:]:
                            hdr.append(html.Th(vType.units or ''))
                        table.append(hdr)
                        if nRecent:
                            # display the most recent rows recorded
                            return keyTable.recent(nRecent).addCallback(
                                appendToHTMLTable,table)
                        elif interval and endAt:
                            # display rows recorded during the specified period
                            return keyTable.byDate(interval,endAt).addCallback(
                                appendToHTMLTable,table)
                        else:
                            # this should never happen for an AJAX post
                            return self.error('Invalid request parameters')
                else:
                    # return a list of valid keywords for this actor
                    names = html.Div()
                    for keyName in sorted(actor.kdict.keys):
                        key = actor.kdict[keyName]
                        # display the keyword name with its original case
                        cased = key.name
                        # only provide links to keywords that have a corresponding
                        # database table (so at least one row of keyword data is available)
                        if database.KeyTable.exists(actorName,keyName):
                            node = html.A(cased,href='?actor=%s&key=%s' % (actorName,cased))
                        else:
                            node = html.Span(cased)
                        # use this keyword's help as a tooltip
                        node['title'] = (key.help or
                            'No help available for the %s.%s keyword' % (actorName,keyName))
                        # separate each keyword with a (breaking) space
                        names.extend([node,' '])
                    return names
        else:
            return 'No actor specified'