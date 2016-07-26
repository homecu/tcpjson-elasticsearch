#!/usr/bin/env python3

import aiohttp
import asyncio
import datetime
import json
import shellish

loop = asyncio.get_event_loop()


class TCPJSONServerProtocol(object):

    def connection_made(self, transport):
        self.transport = transport
        server = transport._server
        self.verbose = server.verbose
        self.es_session = server.es_session
        self.es_url = server.es_url

    def data_received(self, data):
        log = json.loads(data.decode())
        peer = self.transport.get_extra_info('peername')[0]
        if 'timestamp' not in log:
            log['timestamp'] = datetime.datetime.utcnow().isoformat()
        log['host_ip'] = peer
        if self.verbose:
            shellish.vtmlprint('<b>LOG:<b>', log)
        asyncio.ensure_future(self.relaylog(log))

    def eof_received(self):
        pass

    def connection_lost(self, foo):
        pass

    async def relaylog(self, log):
        data = json.dumps(log)
        with aiohttp.Timeout(60):
            async with self.es_session.post(self.es_url, data=data) as r:
                if r.status != 201:
                    raise Exception(await r.text())
                if self.verbose:
                    shellish.vtmlprint('<b>ES INDEX:</b>', await r.text())


@shellish.autocommand
def tcpjson_es_relay(elasticsearch_url, listen_addr='0.0.0.0', listen_port=19831,
                     verbose=False, es_conn_limit=100):
    """ A tcp/json server relay to elasticsearch.

    The URL should contain the /index/type args as per the elasticsearch API.

    E.g. https://elasticsearch/logs/docker
    """
    setup = loop.create_server(TCPJSONServerProtocol, listen_addr, listen_port)
    server = loop.run_until_complete(setup)
    server.verbose = verbose
    conn = aiohttp.TCPConnector(limit=es_conn_limit)
    server.es_session = aiohttp.ClientSession(loop=loop, connector=conn)
    server.es_url = elasticsearch_url
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.close()

tcpjson_es_relay()
