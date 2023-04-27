from collections import defaultdict
from typing import (
    List,
    Any,
    Union,
    Tuple,
    Collection,
    Iterable,
    AsyncIterable,
    Dict,
    Optional,
)

from elasticsearch import helpers, AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError
from pyinline import inline

_TYPE_BULK_ACTION = Union[bytes, str, Dict[str, Any]]


async def async_bulk(
    *args: Any,
    client: AsyncElasticsearch,
    actions: Union[Iterable[_TYPE_BULK_ACTION], AsyncIterable[_TYPE_BULK_ACTION]],
    stats_only: bool = False,
    ignore_status: Union[int, Collection[int]] = (),
    **kwargs: Any,
) -> Tuple[int, Union[int, Dict[str, Any]]]:
    """Helper for the :meth:`~elasticsearch.AsyncElasticsearch.bulk` api that provides

    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks. It returns a tuple with summary
    information - number of successfully executed actions and either list of
    errors or number of errors if ``stats_only`` is set to ``True``. Note that
    by default we raise a ``BulkIndexError`` when we encounter an error so
    options like ``stats_only`` only+ apply when ``raise_on_error`` is set to
    ``False``.

    When errors are being collected original document data is included in the
    error dictionary which can lead to an extra high memory usage. If you need
    to process a lot of data and want to ignore/collect errors please consider
    using the :func:`~elasticsearch.helpers.async_streaming_bulk` helper which will
    just return the errors and not store them in memory.


    :arg client: instance of :class:`~elasticsearch.AsyncElasticsearch` to use
    :arg actions: iterator containing the actions
    :arg stats_only: if `True` only report number of successful/failed
        operations instead of just number of successful and a list of error responses
    :arg ignore_status: list of HTTP status code that you want to ignore

    Any additional keyword arguments will be passed to
    :func:`~elasticsearch.helpers.async_streaming_bulk` which is used to execute
    the operation, see :func:`~elasticsearch.helpers.async_streaming_bulk` for more
    accepted parameters.
    """
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = defaultdict(list)

    # make streaming_bulk yield successful results, so we can count them
    kwargs["yield_ok"] = True
    results: Dict[str, List[str, Any]]
    results = defaultdict(list)
    async_streaming_bulk_itr = helpers.async_streaming_bulk(
        client,
        actions,
        *args,
        ignore_status=ignore_status,
        **kwargs
        # type: ignore[misc]
    )
    while True:
        try:
            # go through request-response pairs and detect failures
            ok, item = await anext(async_streaming_bulk_itr)
        except StopAsyncIteration:
            break
        except BulkIndexError as e:
            ok, item = False, e.errors[0]

        action = list(item.keys())[0]
        if not ok:
            failed += 1
            if not stats_only:
                errors[action].append(item[action])
        else:
            success += 1
            if not stats_only:
                results[action].append(item[action]['_id'])

    return (success, failed) if stats_only else (dict(results), dict(errors))


@inline
def refresh_keyword(r: Optional[bool]):
    return "wait_for" if r is None else str(r).lower()
