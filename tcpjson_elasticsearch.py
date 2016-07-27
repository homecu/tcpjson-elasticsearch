#!/usr/bin/env python3

import aiohttp
import asyncio
import datetime
import json
import shellish

loop = asyncio.get_event_loop()


@shellish.autocommand
def tcpjson_es_relay(elasticsearch_url, listen_addr='0.0.0.0',
                     listen_port=19831, verbose=False, es_conn_limit=100):
    """ A tcp/json server relay to elasticsearch.

    The URL should contain the /index/type args as per the elasticsearch API.

    E.g. https://elasticsearch/logs/docker
    """
    conn = aiohttp.TCPConnector(limit=es_conn_limit)
    es_session = aiohttp.ClientSession(loop=loop, connector=conn)
    es_url = elasticsearch_url

    @asyncio.coroutine
    def on_data(reader, writer):
        while True:
            data = yield from reader.readline()
            if not data:
                break
            log = json.loads(data.decode())
            addr = writer.get_extra_info('peername')[0]
            if 'timestamp' not in log:
                log['timestamp'] = datetime.datetime.utcnow().isoformat()
            log['host_addr'] = addr
            if verbose:
                shellish.vtmlprint('<b>LOG:<b>', log)
            asyncio.ensure_future(relaylog(log))

    async def relaylog(log):
        data = json.dumps(log)
        with aiohttp.Timeout(60):
            async with es_session.post(es_url, data=data) as r:
                if r.status != 201:
                    raise Exception(await r.text())
                if verbose:
                    shellish.vtmlprint('<b>ES INDEX:</b>', await r.text())

    setup = asyncio.start_server(on_data, listen_addr, listen_port, loop=loop)
    server = loop.run_until_complete(setup)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

tcpjson_es_relay()
