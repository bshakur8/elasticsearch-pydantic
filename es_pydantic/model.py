import asyncio
import itertools
from collections import defaultdict
from datetime import datetime
from typing import Optional, Mapping, Any, Sequence
from typing import Type

from elasticsearch import NotFoundError as ElasticNotFoundError, helpers
from pydantic import BaseModel, Field
from pydantic.main import ModelMetaclass
from pyinline import inline

from . import helpers as es_helpers
from .client import get_client
from .response import Response
from .utils import classproperty

MISSING_ID = ValueError("'id' missing from model")


class ESModelMeta(ModelMetaclass):
    """Abstract ESModel Metaclass

    Ensures that any concrete implementations of ESModel
    include all necessary definitions, ex. Meta internal class
    """

    def __new__(cls, name, bases, namespace):
        base_model = ModelMetaclass.__new__(cls, name, bases, namespace)
        meta = base_model.__dict__.get("Meta", False)
        if not meta:
            raise NotImplementedError("Internal 'Meta' class is not implemented")

        try:
            if namespace['Config'].abstract:
                return base_model
        except (AttributeError, KeyError):
            pass

        # Check existence of Meta field names
        mandatory_fields = {
            'index',
        }
        missing = [f for f in mandatory_fields if not meta.__dict__.get(f)]
        if missing:
            raise NotImplementedError(
                f"'{', '.join(missing)}' property is missing from internal Meta class"
            )

        return base_model


class ESModel(BaseModel, metaclass=ESModelMeta):  # noqa
    id: Optional[str] = Field(default=None)

    class Config:
        abstract = True

        extra = "forbid"  # forbid adding undefined fields
        allow_population_by_field_name = False  # force field annotation
        json_encoders = {datetime: lambda dt: dt.isoformat()}

    class Meta:
        index: str
        enable_source: bool
        version: Optional[str]

    class Settings:
        number_of_shards: int
        number_of_replicas: int

    def to_es(self, **kwargs) -> dict:
        self.validate_model()

        exclude_unset = kwargs.pop(
            "exclude_unset",
            False,  # Set as false so that default values are also stored
        )
        exclude: set = {'id'} | kwargs.pop("exclude", {"id"})

        d = self.dict(exclude=exclude, exclude_unset=exclude_unset, **kwargs)

        # Encode datetime fields
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()

        return d

    @classmethod
    def from_es(cls, data: Mapping) -> Optional['ESModel']:
        if not data:
            return None

        source = data.get("_source", {})
        _id = data.get("_id")
        if not source or not _id:
            raise InvalidElasticsearchResponse()

        model = cls(**source)
        model.id = _id

        return model

    @inline
    def validate_model(self):
        # create the same model with the current values
        # it will run all validator again and raise ValidationError in case
        type(self)(**self.dict())
        return True

    async def save(self, index: Optional[str] = None, refresh: Optional[bool] = False):
        # validate current model values state with ES model
        self.validate_model()
        doc = self.dict(exclude={"id"})
        refresh = es_helpers.refresh_keyword(refresh)
        index = index or self.Meta.index
        client = get_client()
        res = await client.index(index=index, body=doc, id=self.id, refresh=refresh)
        self.id = res.get("_id")
        return self.id

    @classmethod
    async def get(cls, _id: str, index: Optional[str] = None):
        index = index or cls.Meta.index
        try:
            client = get_client()
            res = await client.get(index=index, id=_id)
        except ElasticNotFoundError:
            raise NotFoundError(f"document with id {_id} is not found") from None

        return cls.from_es(res)

    async def delete(
        self, index: Optional[str] = None, refresh: Optional[bool] = False
    ):
        if not self.id:
            raise MISSING_ID

        refresh = es_helpers.refresh_keyword(refresh)
        index = index or self.Meta.index
        try:
            client = get_client()
            await client.delete(index=index, id=self.id, refresh=refresh)
        except ElasticNotFoundError:
            raise NotFoundError(f"document with id={self.id} is not found") from None

    @classmethod
    async def search(cls, query: dict):
        client = get_client()
        response = await client.search(index=cls.Meta.index, **query)
        return Response(query, response, model=cls)

    @classmethod
    async def bulk_index(cls, collection):
        async with Session(refresh=True) as session:
            session.bulk_index(collection=collection, index=cls.Meta.index)
            results = await session.commit()
        return results['index']

    @classmethod
    @classproperty
    def index(cls):
        return cls._Index(model=cls)

    class _Index:
        # Operations on elasticsearch Index

        __slots__ = {"_model", "_client", "_index_name"}

        def __init__(self, model: Type['ESModel']):
            self._model = model
            self._client = get_client()

            # shortcuts
            self._index_name = model.Meta.index

        @inline
        def index_template_format(self) -> dict:
            template = {
                "template": {
                    "mappings": self.mappings(),
                    "settings": self.settings(),
                },
                "index_patterns": [self.pattern()],
                "name": self._index_name,
                "composed_of": [],
                "order": None,
                "priority": 1,
            }
            try:
                template["version"] = self._model.Meta.version
            except AttributeError:
                ...
            return {t: v for t, v in template.items() if v}

        @inline
        def pattern(self):
            return self._index_name + "-*"

        @inline
        def alias(self) -> str:
            return self._index_name

        @inline
        def settings(self):
            settings_cls = self._model.Settings

            def index():
                idx_props = {'number_of_shards', 'number_of_replicas'}
                idx_settings = {p: getattr(settings_cls, p, None) for p in idx_props}
                return {p: v for p, v in idx_settings.items() if v}

            settings = {
                "index": index(),
            }
            return {s: v for s, v in settings.items() if v}

        @inline
        def mappings(self):
            fields = self._model.__dict__['__annotations__'].items()
            properties = {name: field.to_dict() for name, field in fields}
            mappings = {
                "properties": {p: v for p, v in properties.items() if v},
            }

            try:
                mappings["_source"] = {"enabled": self._model.Meta.enable_source}
            except AttributeError:
                pass

            return mappings

        async def delete(self):
            ignore_status = [404]
            _client__ = self._client.options(ignore_status=ignore_status)
            result = await _client__.indices.get_alias(index=self._index_name)
            if result.meta.status not in ignore_status:
                tasks = (self._client.indices.delete(index=idx) for idx in result.body)
                await asyncio.gather(*tasks)

        @inline
        async def exist(self, **kwargs) -> bool:
            """Returns True if the index already exists in elasticsearch.

            Any additional keyword arguments will be passed to
            ``AsyncElasticsearch.indices.exists`` unchanged.
            """
            return await self._client.indices.exists(index=self._index_name, **kwargs)

        async def setup(self, move_data=True, update_alias=True, force_migrate=False):
            exist = await self._client.indices.exists_index_template(
                name=self._index_name
            )
            if not exist:
                # upload the template into elasticsearch.
                # potentially overriding the one already there
                await self._client.indices.put_index_template(
                    **self.index_template_format()
                )

            force_migrate = force_migrate or (not await self.exist())

            if force_migrate:
                await self.migrate(move_data=move_data, update_alias=update_alias)

        @inline
        async def migrate(self, move_data=True, update_alias=True):
            next_index = self.pattern().replace(
                "*", datetime.utcnow().strftime("%Y%m%d-%H%M%S%f")
            )

            # create new index, it will use the settings from the template
            res = await self._client.indices.create(index=next_index)
            index = res.body['index']

            if move_data:
                # move data from current alias to the new index
                await helpers.async_reindex(
                    client=self._client.options(ignore_status=404),
                    source_index=self.alias(),
                    target_index=next_index,
                )
                # refresh the index to make the changes visible
                await self._client.indices.refresh(index=next_index)

            if update_alias:
                # repoint the alias to point to the newly created index
                await self._client.indices.update_aliases(
                    actions=[
                        {"remove": {"alias": self.alias(), "index": self.pattern()}},
                        {"add": {"alias": self.alias(), "index": next_index}},
                    ]
                )
            return index


Item = Mapping[str, Any]
CollectionItem = Mapping[str, Item]
Collection = Sequence[CollectionItem]
ModelCollection = Sequence[ESModel]


class Session:
    __slots__ = {"actions", "_refresh", "model", "results"}

    def __init__(self, refresh: Optional[bool] = False):
        self.actions = defaultdict(list)
        self._refresh = refresh

        self.model = self.SessionModel(s=self)
        self.results = []

    async def __aenter__(self):
        self.actions.clear()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return

        # I preferred not to commit here, because there will be no way to take
        # the results unless we save them locally and I don't want to do that.
        # >> await self.commit()

    async def commit(
        self,
        refresh: Optional[bool] = False,
        chunk_size: Optional[int] = None,
        async_bulk_kwargs=None,
    ) -> dict:
        if not self.actions:
            return {}

        async_bulk_kwargs = async_bulk_kwargs or {}
        refresh = es_helpers.refresh_keyword(refresh or self._refresh)
        client = get_client()
        kwargs = {
            'client': client.raw_connection(),
            'actions': list(itertools.chain(*self.actions.values())),
            'chunk_size': chunk_size,
            'refresh': refresh,
            'raise_on_error': False,
            'stats_only': False,
        }
        # order matters: kwargs keys will be preferred
        kwargs = {**async_bulk_kwargs, **kwargs}
        kwargs = {k: v for k, v in kwargs.items() if v}

        try:
            self.results, errors = await es_helpers.async_bulk(**kwargs)
        finally:
            self.actions.clear()
        if errors:
            raise SessionError(errors)
        return self.results

    @inline
    def bulk_index(self, collection: Collection, index: str):
        return [self.index(model, index=index) for model in collection]

    @inline
    def _action(self, name, action):
        # returns result index to match it with action later
        self.actions[name].append(action)
        return len(self.actions[name]) - 1

    def index(self, body: Mapping, index: str, extra=None):
        extra = extra or {}
        # no id needed, ES will generate one
        request = {"_index": index, "_op_type": "index", "_source": body, **extra}
        return self._action("index", request)

    def delete(self, _id: str, index: str):
        request = {"_id": _id, "_index": index, "_op_type": "delete"}
        return self._action("delete", request)

    def create(self, body: dict, _id: str, index: str):
        request = {
            "_id": _id,
            "_index": index,
            "_op_type": "create",
            "_source": body,
        }
        return self._action("create", request)

    def update(self, body: dict, _id: str, index: str):
        request = {
            "_index": index,
            "_op_type": "update",
            "_source": {"doc": body},
            '_id': _id,
        }
        return self._action("update", request)

    class SessionModel:
        __slots__ = {"session"}

        def __init__(self, s: 'Session'):
            self.session = s

        async def bulk_index(
            self, models: ModelCollection, commit_kwargs=None
        ) -> ModelCollection:
            for model in models:
                model.validate_model()
            model_indices = [self.index(model=model) for model in models]

            commit_kwargs = commit_kwargs or {}
            results = await self.session.commit(**commit_kwargs)
            for i, created_id in zip(model_indices, results["index"], strict=True):
                models[i].id = created_id

            return models

        def index(self, model: ESModel, extra=None) -> int:
            extra = extra or {}
            body = model.to_es()
            return self.session.index(body=body, index=model.Meta.index, extra=extra)

        def update(self, model: ESModel):
            # update means reindex the document with given id
            if not model.id:
                raise MISSING_ID

            body = model.to_es()
            return self.session.update(_id=model.id, body=body, index=model.Meta.index)

        def delete(self, model: ESModel):
            if not model.id:
                raise MISSING_ID
            model.validate_model()

            return self.session.delete(_id=model.id, index=model.Meta.index)

        def create(self, model: ESModel):
            if not model.id:
                raise MISSING_ID

            body = model.to_es()
            return self.session.create(body=body, _id=model.id, index=model.Meta.index)


class ElasticError(Exception):
    ...


class InvalidElasticsearchResponse(ElasticError):
    ...


class NotFoundError(ElasticError):
    ...


class SessionError(ElasticError):
    __Slots__ = {"errors"}

    def __init__(self, errors):
        self.errors = errors
