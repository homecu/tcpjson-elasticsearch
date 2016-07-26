tcpjson-elasticsearch
========

A trivial asyncio json/tcp server to elasticsearch relay


Usage
--------
```
python3.5 ./tcpjson_elasticsearch.py https://elasticsearch.location.foo:12345/your_index/your_type
```

Then you can direct tcp clients to `tcp://localhost:19831` a la..
```
docker run -v /var/run/docker.sock:/var/run/docker.sock \
    jmayfield/docker-stats -- --statsinterval 60 --host localhost:19831
```
