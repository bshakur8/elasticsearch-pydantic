from elasticsearch import AsyncElasticsearch

__all__ = ["connect", "disconnect", "get_client", "ClientES"]

from pyinline import inline


class ClientES:
    _client: AsyncElasticsearch = None

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __getattr__(self, item):
        try:
            return getattr(self._client, item)
        except AttributeError:
            raise AttributeError(
                "client not initialized - make sure to call connect()"
            ) from None

    def connect(self, *args, **kwargs):
        self._args = args or self._args
        self._kwargs = kwargs or self._kwargs
        self._client = AsyncElasticsearch(*self._args, **self._kwargs)
        return self._client

    @inline
    def raw_connection(self) -> AsyncElasticsearch:
        return self._client

    async def close(self):
        await self._client.close()

    async def __aenter__(self):
        self.connect()
        global client
        client = self
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return
        await self.close()


client = ClientES()

connect = client.connect
disconnect = client.close


@inline
def get_client():
    return client
