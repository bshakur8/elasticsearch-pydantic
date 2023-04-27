import datetime
import uuid

import pytest
from pydantic import ValidationError, ConfigError

from es_pydantic import validators
from es_pydantic.client import ClientES
from es_pydantic.field import Text
from es_pydantic.model import ESModel, InvalidElasticsearchResponse, NotFoundError
from . import EventLog, Shirt
from .conftest import NOW, shirts_generator, events_generator, match_all

pytestmark = pytest.mark.asyncio


def test_model_definition_without_meta_class():
    with pytest.raises(NotImplementedError):

        class MyModel(ESModel):
            ...

        MyModel()


def test_model_definition_without_index_name():
    with pytest.raises(NotImplementedError):

        class MyModel(ESModel):
            class Meta:
                ...

        MyModel()


def test_model_missing_fields_raises_validation_error():
    with pytest.raises(ValidationError):
        Shirt(brand="Gucci")


def test_model_extra_fields_raises_validation_error():
    with pytest.raises(ValidationError):
        Shirt(brand="Gucci", color="red", model="slim")


def test_model_invalid_color_raises_validation_error():
    with pytest.raises(ValidationError):
        Shirt(brand="Gucci", color="blue")


async def test_model_save_check_id_is_string(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)
    assert type(shirt.id) == str

    shirts = await match_all(Shirt)
    assert len(shirts) == 1
    assert shirt in shirts


async def test_model_save_with_index(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)
    res = await client.get(index=shirt.Meta.index, id=shirt.id)
    assert res["found"]

    model = shirt.to_es()
    assert model == res['_source']


async def test_model_update(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)

    shirt.brand = "Boss"
    await shirt.save(refresh=True)

    shirts = await match_all(Shirt)
    assert len(shirts) == 1
    assert shirt in shirts


async def test_model_update_validation_error(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)
    shirt.color = "purple"
    with pytest.raises(ValidationError):
        await shirt.save()


async def test_model_update_extra_fields(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)
    with pytest.raises(ValueError):
        shirt.no_such_field = "123"


async def test_model_get(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)

    shirt2 = await Shirt.get(shirt.id)
    assert id(shirt) != id(shirt2)
    assert shirt == shirt2
    assert shirt.to_es() == shirt2.to_es()
    assert shirt.__fields__ == shirt2.__fields__


async def test_model_from_es(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)

    es_response = await client.get(id=shirt.id, index=shirt.Meta.index)
    new_shirt = Shirt.from_es(es_response)
    assert shirt == new_shirt


def test_model_empty_data():
    assert Shirt.from_es({}) is None


def test_model_from_es_invalid_format():
    res = {"does not": "include _source", "or": "_id"}

    with pytest.raises(InvalidElasticsearchResponse):
        Shirt.from_es(res)


async def test_model_to_es(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)

    es_from_shirt = shirt.to_es()

    res = await client.get(index=shirt.Meta.index, id=shirt.id)
    assert res["_source"] == es_from_shirt


async def test_model_to_es_with_exclude(client: ClientES):
    shirt = Shirt(brand="Gucci", color="black")
    await shirt.save(refresh=True)
    es_from_shirt = shirt.to_es(exclude={"color"})

    # Check that id excluded and fields excluded
    assert es_from_shirt == {"brand": "Gucci"}


async def test_model_get_with_dynamic_index(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(index="custom", refresh=True)

    get = await Shirt.get(index="custom", _id=shirt.id)
    assert get == shirt


async def test_model_delete_raises_error(client: ClientES):
    shirt = next(shirts_generator)
    # no save
    with pytest.raises(NotFoundError):
        await shirt.delete(refresh=True)

    shirt.id = "abcdef"
    with pytest.raises(NotFoundError):
        await shirt.delete(refresh=True)


async def test_model_delete(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)
    await shirt.delete(refresh=True)

    with pytest.raises(NotFoundError):
        await Shirt.get(shirt.id)


async def test_model_delete_with_dynamic_index(client: ClientES):
    shirt = next(shirts_generator)
    await shirt.save(refresh=True)

    custom_index = "abc"

    random_shirt = next(shirts_generator)
    await random_shirt.save(index=custom_index, refresh=True)
    await random_shirt.delete(index=custom_index, refresh=True)

    with pytest.raises(NotFoundError):
        await Shirt.get(random_shirt.id, index=custom_index)

    # verify if still exist is original index
    shirt2 = await Shirt.get(shirt.id, index=shirt.Meta.index)
    assert shirt2 == shirt


async def test_internal_meta_class_changes_limited_to_instance(client: ClientES):
    # Cannot modify Meta index to have a dynamic index name
    shirt = next(shirts_generator)
    shirt.Meta.index = "user-index"
    await shirt.save(refresh=True)

    assert Shirt.Meta.index == "user-index"
    assert shirt.Meta.index == "user-index"


async def test_model_save_datetime_serialize_datetime_request(client: ClientES):
    ts = NOW - datetime.timedelta(days=5)
    event = EventLog(timestamp=ts, guid=uuid.uuid4(), object_type="volume")
    await event.save(refresh=True)

    assert event.timestamp == ts

    events = await match_all(EventLog)
    assert len(events) == 1
    assert event in events


async def test_model_save_datetime_serialize_datetime_response(client: ClientES):
    event_log = next(events_generator)
    # set time manually so we can check it
    event_log.timestamp = NOW
    await event_log.save(refresh=True)

    assert type(event_log.timestamp) == datetime.datetime
    assert event_log.timestamp == NOW


def test_model_invalid_validator():
    with pytest.raises(ConfigError):

        class MyModel(ESModel):
            guid: Text()

            v1 = validators.guid("guid")
            v2 = validators.guid("different_guid_field_name")

        MyModel(guid=uuid.uuid4())
