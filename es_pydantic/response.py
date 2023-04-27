from collections import UserDict
from typing import TypeVar, List

M = TypeVar("M", bound='Response')


class Response(UserDict):
    __slots__ = {"response", "_query", "model"}

    def __init__(self, query, response, *, model: M):
        super().__init__(response.get('hits', {}))
        self.response = response
        self._query = query
        self.model: M = model

    def __len__(self):
        return len(self.documents)

    def __repr__(self):
        return f"<Response: {self.data}> {self.buckets}"

    @property
    def hits(self) -> dict:
        return self.data

    @property
    def shards(self) -> dict:
        return self.response['_shards']

    @property
    def timed_out(self) -> bool:
        return self.response['timed_out']

    def success(self) -> bool:
        return not self.timed_out and self.shards['total'] == self.shards['successful']

    @property
    def documents(self) -> List[M]:
        sources = self.data.get('hits', [])
        return [self.model(id=src['_id'], **src['_source']) for src in sources]

    @property
    def fields(self):
        try:
            docs = self.data.get('hits', [])
            return list(docs[0].get('fields', {}))
        except IndexError:
            return []

    @property
    def buckets(self):
        try:
            return {
                n: self.response['aggregations'][n]["buckets"]
                for n in self._query['aggs']
            }
        except (KeyError, IndexError):
            return None
