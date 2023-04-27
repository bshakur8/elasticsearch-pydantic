import asyncio
import contextlib
import itertools
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import List

from pydantic import validator

import field
import validators
from client import ClientES, connect, disconnect
from model import ESModel, NotFoundError
from response import Response

DEFAULT_DATETIME_FORMAT = "strict_date_optional_time_nanos||epoch_millis"


class VmsDockerLogs(ESModel):
    timestamp: field.DateTime(format=DEFAULT_DATETIME_FORMAT)  # noqa: F722
    message: field.Text()
    node: field.Keyword()
    cluster_build: field.Text()
    cluster_sw_version: field.Version()
    cluster_name: field.Text()
    cluster_guid: field.Keyword()
    cluster_psnt: field.Text()

    class Meta:
        index = "vms_docker_logs_index"
        enable_source = True
        version = 1

    class Settings:
        number_of_shards = 2
        number_of_replicas = 1

    # validators
    v1 = validators.guid("cluster_guid", always=True)


class VmsEventsModel(ESModel):
    timestamp: field.DateTime(format=DEFAULT_DATETIME_FORMAT)
    object_type: field.Text()  # Snapshot
    object_id: field.Integer()  # 151
    event_type: field.Text()  # OBJECT_DELETED
    event_origin: field.Text()  # USER
    severity: field.Text()  # INFO
    event_message: field.Text()  # Manager: root (10.71.15.111) DELETE object...
    cluster_guid: field.Keyword(ignore_above=256)  #
    cluster_name: field.Text()  # vast-17
    cluster_psnt: field.Text()  # vast-17
    location: field.Text()  # Haifa
    site: field.Text()  # Haifa
    customer: field.Text()  # QA
    event_id: field.Integer()  # 16

    class Meta:
        index = "vms_events_index"
        enable_source = True
        version = 1

    class Settings:
        number_of_shards = 2
        number_of_replicas = 1

    # validators
    v1 = validators.guid("cluster_guid", always=True)


class Shirts(ESModel):
    timestamp: field.DateTime()
    brand: field.Keyword()
    color: field.Keyword()
    model: field.Keyword()

    class Meta:
        index = "shirts"
        enable_source = True
        version = 1

    class Settings:
        number_of_shards = 1
        number_of_replicas = 1

    @validator("color", always=True)
    @classmethod
    def validate_color(cls, color):
        if color not in ["black", "red"]:
            raise ValueError("Invalid color")
        return color


ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "*Zn6gNEjQw0PzDbEPrCN")
credentials = {
    'hosts': "https://localhost:9200",
    'ca_certs': "/tmp/http_ca.crt",
    'basic_auth': ("elastic", ELASTIC_PASSWORD),
}


async def add_shirts():
    models = []
    for brand, color, model in itertools.product(
        ["gucci", "armani"], ["red", "black"], ["slim", "fat"]
    ):
        days = random.randint(1, 9)
        hours = random.randint(1, 20)
        ts = datetime.utcnow() - timedelta(days=days) + timedelta(hours=hours)
        models.append(Shirts(timestamp=ts, brand=brand, color=color, model=model))

    async with Shirts.session(refresh=True) as session:
        await session.model.bulk_index(models)

    return models


async def test_shirts_sanity():
    schemes = [Shirts]
    async with ClientES(**credentials):
        await clean(schemes=schemes)
        await setup(schemes=schemes)

        await add_shirts()

        query = {
            "query": {
                "bool": {
                    "filter": [{"term": {"color": "red"}}, {"term": {"brand": "gucci"}}]
                }
            },
        }
        response: Response = await Shirts.search(query)
        assert not response.timed_out
        assert len(response) == len(query['query']['bool']['filter'])
        assert response.success()
        assert response.fields == []
        assert response.buckets is None


async def test_shirts_aggregations():
    schemes = [Shirts]
    async with ClientES(**credentials):
        await clean(schemes=schemes)
        await setup(schemes=schemes)

        await add_shirts()

        query = {
            "query": {
                "bool": {
                    "filter": [{"term": {"color": "red"}}, {"term": {"brand": "gucci"}}]
                }
            },
            "aggs": {"models": {"terms": {"field": "model"}}},
        }
        response: Response = await Shirts.search(query)
        assert not response.timed_out
        assert len(response) == len(query['query']['bool']['filter'])
        assert response.success()
        assert response.fields == query.get('fields', [])
        for t in [{'key': 'fat', 'doc_count': 1}, {'key': 'slim', 'doc_count': 1}]:
            assert t in response.buckets['models']


def generate_doc():
    _id = 0
    now = datetime.utcnow()
    g1, g2 = uuid.uuid4(), uuid.uuid4()
    while True:
        _id += 1
        ts = now - timedelta(days=random.randint(1, 5), hours=random.randint(0, 23))
        message = "".join(random.sample(string.ascii_letters, k=30))
        node = f"{random.choice(['c', 'd'])}node{random.randint(1, 5)}"
        psnt = f"vast-{random.choice(['17', '164'])}"
        guid = g1 if psnt.endswith("17") else g2
        yield VmsDockerLogs(
            id=_id,
            timestamp=ts.isoformat(),
            message=message,
            node=node,
            cluster_build=f"release/4.{random.choice([4, 6])}.0",
            cluster_sw_version="1.2.3.4",
            cluster_name=psnt,
            cluster_guid=str(guid),
            cluster_psnt=psnt,
        )


docs_generator = generate_doc()


# Tests
async def clean(schemes: List = None):
    schemes = schemes or [VmsEventsModel, VmsDockerLogs, Shirts]
    print("clean Indices")
    for scheme in schemes:
        await scheme.index.delete()


async def setup(schemes: List):
    for scheme in schemes:
        index = scheme.index
        index_name = scheme.Meta.index
        exist = await index.exist()
        print(f"Index {index_name} {exist=}")
        await index.setup()
        exist = await index.exist()
        print(f"Index {index_name} {exist=}")


async def add_many_docs():
    logs = [next(docs_generator).document for _ in range(10)]
    docs = await VmsDockerLogs.bulk_index(logs)
    print(docs)

    logs = [next(docs_generator).document for _ in range(10)]
    async with VmsDockerLogs.session(refresh=True) as session:
        docs = session.bulk_index(logs)

    print(docs)

    logs = [next(docs_generator) for _ in range(10)]
    async with VmsDockerLogs.session() as session:
        models = await session.model.bulk_index(logs)
        await session.commit(refresh=True, chunk_size=1)

    print(models)


async def add_docs_session():
    document = next(docs_generator)
    document2 = next(docs_generator)

    async with VmsDockerLogs.session(refresh=True) as session:
        session.model.create(document)
        document.cluster_name = "vast-124"
        document.node = "cnode-91"
        document2.node = "dnode-7"
        document2.cluster_name = "vast-199"
        session.model.update(document)
        session.model.update(document2)
        results = await session.commit()

        session.model.index(document2)
        document2.id = results['index'][-1]
        document2.cluster_name = "vast-12"
        session.model.update(document2)

    doc2 = await VmsDockerLogs.get(_id=document2.id)
    assert doc2 == document2


async def add_docs_model():
    doc1 = next(docs_generator)
    await doc1.save(refresh=True)

    doc1.node = "dnode-13"
    await doc1.save(refresh=True)

    doc1.cluster_sw_version = "7.8.9.0"
    await doc1.save(refresh=True)

    doc2 = next(docs_generator)
    await doc2.save(refresh=True)

    doc1tag = await VmsDockerLogs.get(_id=doc1.id)
    assert doc1.dict().items() == doc1tag.dict().items()

    await doc2.delete()
    with contextlib.suppress(NotFoundError):
        await doc2.delete()

    with contextlib.suppress(NotFoundError):
        await VmsDockerLogs.get(_id=doc2.id)

    await doc2.save(refresh=True)

    doc2tag = await VmsDockerLogs.get(_id=doc2.id)
    assert doc2 == doc2tag


async def session_models():
    doc1 = next(docs_generator)
    doc2 = next(docs_generator)
    doc3 = next(docs_generator)
    doc4 = next(docs_generator)
    async with VmsDockerLogs.session(refresh=True) as session:
        iddoc2 = session.model.index(doc2)
        session.model.create(doc1)
        session.model.create(doc3)
        iddoc4 = session.model.index(doc4)

        doc1.cluster_sw_version = "1.2.3.4"
        results = await session.commit(refresh=True)

        id_doc1 = session.model.update(doc1)
        assert id_doc1 == 0

        doc2.id = results['index'][iddoc2]
        doc4.id = results['index'][iddoc4]

        doc1.node = "cnode-2"
        session.model.update(doc1)
        doc1.cluster_sw_version = "7.8.9.10"
        session.model.update(doc1)
        session.model.delete(doc2)

    doc1tag = await doc1.get(_id=doc1.id)
    assert doc1 == doc1tag


async def search():
    aggr_name = "node_agg"
    sub_aggr = "max_timestamp"
    query = {
        "fields": ["node", "timestamp"],
        "query": {
            "bool": {
                "must": [
                    {"match": {"cluster_psnt": "vast-123"}},
                ]
            }
        },
        "aggs": {
            aggr_name: {
                "terms": {
                    "field": "node",
                },
                "aggs": {
                    sub_aggr: {
                        "max": {
                            "field": "timestamp",
                        },
                    },
                },
            },
        },
    }
    response = await VmsDockerLogs.search(query)
    print(response.buckets)


async def demo_global_connection():
    schemes = [VmsEventsModel, VmsDockerLogs]
    try:
        connect(**credentials)
        await clean(schemes=schemes)
        await setup(schemes=schemes)

        await add_many_docs()
        await add_docs_model()
        await add_docs_session()
        await session_models()
        await search()

    finally:
        await disconnect()


async def demo_context_manager():
    schemes = [VmsEventsModel, VmsDockerLogs]
    async with ClientES(**credentials):
        await clean(schemes=schemes)
        await setup(schemes=schemes)

        await add_docs_model()
        await add_docs_session()
        await session_models()
        await search()


if __name__ == '__main__':
    asyncio.run(demo_global_connection())
    asyncio.run(demo_context_manager())
    asyncio.run(test_shirts_sanity())
    asyncio.run(test_shirts_aggregations())
