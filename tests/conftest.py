import datetime
import itertools
import os
import random
import uuid
from typing import Optional, List, Type

import pytest

from es_pydantic.client import ClientES
from es_pydantic.model import ESModel
from . import EventLog, Shirt

NOW = datetime.datetime.now()

ELASTIC_HOSTS = os.getenv("ELASTIC_HOSTS", "https://localhost:9200")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "wchuEoz03r4ZY9b5Fn1z")
ELASTIC_CA_CERTS = os.getenv("ELASTIC_CA_CERTS", "/tmp/http_ca.crt")

credentials = {
    'hosts': ELASTIC_HOSTS,
    'ca_certs': ELASTIC_CA_CERTS,
    'basic_auth': ("elastic", ELASTIC_PASSWORD),
}


async def clean_records(client):
    await client.delete_by_query(
        index="_all",
        body={"query": {"match_all": {}}},
        wait_for_completion=True,
        refresh=True,
    )


@pytest.fixture()
async def client():
    async with ClientES(**credentials) as client:
        # clean before and after test
        await clean_records(client)
        await Shirt.index.setup()
        yield client
        # keep records in case of failure
        # await clean_records(client)


def random_timedelta_func():
    return {'days': random.randint(1, 5), 'hours': random.randint(0, 23)}


def generate_events(
    guids: List[uuid.UUID] = None, seed=1234, timedelta_func: Optional[callable] = None
):
    random.seed(seed)
    guids = guids or [uuid.uuid4(), uuid.uuid4()]
    types = ["cluster", "node", "volume", "snapshot"]
    while True:
        ts = NOW
        if timedelta_func:
            ts -= datetime.timedelta(**timedelta_func())

        yield EventLog(
            timestamp=ts, guid=random.choice(guids), object_type=random.choice(types)
        )


def generate_shirts(seed=1234, start_from_id=0):
    random.seed(seed)
    brands = ["Gucci", "Nike", "TNT", "Adidas", "Armani", "Zara"]
    colors = ["red", "black"]
    for _id in itertools.count(start_from_id):
        yield Shirt(id=_id, brand=random.choice(brands), color=random.choice(colors))


async def match_all(model_class: Type[ESModel], raw=False):
    query = {"query": {"match_all": {}}}
    result = await model_class.search(query)
    if raw:
        return result

    models = []
    for hits in result['hits']:
        _id = hits['_id']
        models.append(model_class(id=_id, **hits['_source']))
    return models


events_generator = generate_events(timedelta_func=random_timedelta_func)
shirts_generator = generate_shirts()
