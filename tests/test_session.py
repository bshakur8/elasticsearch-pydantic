import pytest

from es_pydantic.client import ClientES
from es_pydantic.model import NotFoundError, Session, SessionError
from .conftest import events_generator, shirts_generator, match_all
from . import EventLog, Shirt

pytestmark = pytest.mark.asyncio


async def test_session_native_bulk_index(client):
    # assume having list of dicts of data to save
    events = [next(events_generator).to_es() for _ in range(10)]

    async with Session(refresh=True) as session:
        for body in events:
            session.index(body=body, index=EventLog.Meta.index)
        commit_result = await session.commit()

    assert len(commit_result) == 1  # one operation
    assert "index" in commit_result
    assert type(commit_result['index']) == list  # preserve order
    assert type(commit_result['index'][0]) == str
    assert len(commit_result['index']) == len(events)

    _id = commit_result['index'][0]
    response = await client.get(index=EventLog.Meta.index, id=_id)
    assert response["found"]

    event_from_es = EventLog.from_es(response)
    event_from_model = await EventLog.get(_id=_id)
    assert event_from_model == event_from_es


async def test_session_model_bulk_index(client):
    # given list of models
    events = [next(events_generator) for _ in range(10)]

    # this is how we can bulk them
    async with Session(refresh=True) as session:
        models = await session.model.bulk_index(events)

    # id was set automatically
    assert all(event.id is not None for event in models)
    # models were saved
    assert models == events


async def test_session_delete(client):
    shirt = Shirt(brand="Adidas", color="black")
    await shirt.save()

    async with Session(refresh=True) as session:
        session.model.delete(shirt)
        await session.commit()

    with pytest.raises(NotFoundError):
        await Shirt.get(shirt.id)


async def test_session_update_without_id_raises_error(client):
    shirt = Shirt(brand="Adidas", color="black")
    # no save
    async with Session() as session:
        shirt.color = "red"
        with pytest.raises(ValueError):
            session.model.update(shirt)


async def test_session_delete_without_id_raises_error():
    shirt = Shirt(brand="Adidas", color="black")

    async with Session(refresh=True) as session:
        with pytest.raises(ValueError):
            session.model.delete(shirt)
            await session.commit()


async def test_session_update(client: ClientES):
    shirt = Shirt(brand="Adidas", color="black")
    await shirt.save(refresh=True)

    async with Session(refresh=True) as session:
        shirt.brand = "Zara"
        session.model.update(shirt)
        await session.commit()

    new_shirt = await Shirt.get(_id=shirt.id)
    assert new_shirt.id == shirt.id
    assert new_shirt.brand == shirt.brand == "Zara"


async def test_add_many_shirts(client: ClientES):
    shirts = [next(shirts_generator).to_es() for _ in range(10)]
    docs_ids = await Shirt.bulk_index(shirts)
    assert len(docs_ids) == len(shirts)
    result = await match_all(Shirt, raw=True)
    assert len(result) == len(shirts)
    assert all(doc.id in docs_ids for doc in result.documents)


async def test_session_with_bulk_error_without_raise_on_error(client):
    shirt = Shirt(id="1", brand="Nike", color="red")  # Not saved

    async with Session(refresh=True) as session:
        session.model.create(shirt)

        # nothing is committed yet
        with pytest.raises(NotFoundError):
            await Shirt.get(_id=shirt.id)

        await session.commit()

    new_shirt = await Shirt.get(_id=shirt.id)
    assert new_shirt == shirt


async def test_session_update_with_bulk_error(client):
    shirt = Shirt(brand="TNT", color="black")  # Not saved

    async with Session() as session:
        with pytest.raises(ValueError):
            session.model.update(shirt)


async def test_session_model_delete_with_bulk_error(client):
    shirts = [next(shirts_generator) for _ in range(10)]

    async with Session() as session:
        for shirt in shirts:
            session.model.delete(shirt)
        with pytest.raises(SessionError) as exp:
            await session.commit()

    assert type(exp.value) is SessionError
    exp_info = exp.value.errors
    exp_details = exp_info['delete'][0]
    assert exp_details['status'] == 404
    assert exp_details['result'] == "not_found"
