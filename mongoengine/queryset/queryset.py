from __future__ import absolute_import
from mongoengine.errors import OperationError
from mongoengine.queryset.base import (BaseQuerySet, DO_NOTHING, NULLIFY,
                                       CASCADE, DENY, PULL)
from mongoengine.base.proxy import LazyPrefetchBase
from collections import defaultdict
from six.moves import range

__all__ = ('QuerySet', 'QuerySetNoCache', 'DO_NOTHING', 'NULLIFY', 'CASCADE',
           'DENY', 'PULL')

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20
ITER_CHUNK_SIZE = 100
MAX_PARALLEL_CHUNKS = 10
MIN_CHUNK_SIZE = 100


class QuerySet(BaseQuerySet, LazyPrefetchBase):
    """The default queryset, that builds queries and handles a set of results
    returned from a query.

    Wraps a MongoDB cursor, providing :class:`~mongoengine.Document` objects as
    the results.
    """

    _has_more = True
    _len = None
    _result_cache = None
    _reference_cache_count = None
    _reference_cache = None
    _parallel_processing_enabled = False
    _parallel_max_chunks = MAX_PARALLEL_CHUNKS
    _parallel_chunk_size = MIN_CHUNK_SIZE

    def next(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        Override parent function for lazy pre-fetching.
        """
        if self._limit == 0 or self._none:
            raise StopIteration

        raw_doc = next(self._cursor)
        if self._as_pymongo:
            return self._get_as_pymongo(raw_doc)

        return self._get_document(self, raw_doc)

    def _get_document(self, raw_doc):
        """
        Should be thread safe
        :param raw_doc:
        :return:
        """
        doc = self._document._from_son(
            raw_doc,
            _auto_dereference=self._auto_dereference,
            only_fields=self.only_fields,
            _lazy_prefetch_base=self,
            _fields=[],
        )

        if self._scalar:
            return self._get_scalar(doc)

        return doc

    def __iter__(self):
        """Iteration utilises a results cache which iterates the cursor
        in batches of ``ITER_CHUNK_SIZE``.

        If ``self._has_more`` the cursor hasn't been exhausted so cache then
        batch.  Otherwise iterate the result_cache.
        """
        self._iter = True
        if self._has_more:
            return self._iter_results()

        # iterating over the cache.
        return iter(self._result_cache)

    def __len__(self):
        """Since __len__ is called quite frequently (for example, as part of
        list(qs)), we populate the result cache and cache the length.
        """
        if self._len is not None:
            return self._len
        if self._has_more:
            # populate the cache
            list(self._iter_results())

        self._len = len(self._result_cache)
        return self._len

    def __repr__(self):
        """Provides the string representation of the QuerySet
        """
        if self._iter:
            return '.. queryset mid-iteration ..'

        self._populate_cache()
        data = self._result_cache[:REPR_OUTPUT_SIZE + 1]
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def _iter_results(self):
        """A generator for iterating over the result cache.

        Also populates the cache if there are more possible results to yield.
        Raises StopIteration when there are no more results"""
        if self._result_cache is None:
            self._result_cache = []
        if self._reference_cache_count is None:
            self._reference_cache_count = defaultdict(int)
        if self._reference_cache is None:
            self._reference_cache = defaultdict(dict)

        pos = 0
        while True:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos += 1
            if not self._has_more:
                raise StopIteration
            if len(self._result_cache) <= pos:
                self._populate_cache()

    def _populate_cache(self):
        """
        Populates the result cache with ``ITER_CHUNK_SIZE`` more entries
        (until the cursor is exhausted).
        """
        if self._result_cache is None:
            self._result_cache = []
        if self._reference_cache_count is None:
            self._reference_cache_count = defaultdict(int)
        if self._reference_cache is None:
            self._reference_cache = defaultdict(dict)

        if self._has_more:
            if self._parallel_processing_enabled is False:
                try:
                    for i in range(ITER_CHUNK_SIZE):
                        self._result_cache.append(next(self))
                except StopIteration:
                    self._has_more = False
            else:
                def process_chunk(raw_docs, result_docs):
                    for raw_doc in raw_docs:
                        role = self._document._from_son(raw_doc, _auto_dereference=False)
                        result_docs.append(role)

                from threading import Thread
                raw_docs = [d for d in q._cursor]  # TODO: What if cursor is consumed partially so far???
                n_chunks = (len(raw_docs) + self._parallel_chunk_size - 1) / self._parallel_chunk_size
                if n_chunks > self._parallel_max_chunks:
                    chunk_size = (len(raw_docs) + self._parallel_max_chunks - 1) / self._parallel_max_chunks

                threads = []
                results = []
                for i in range(0, len(raw_docs), chunk_size):
                    sub_raw_docs = raw_docs[i:i + chunk_size]
                    result = []
                    t = Thread(target=process_chunk, args=(sub_raw_docs, result,))
                    t.start()
                    threads.append(t)
                    results.append(result)

                for t in threads:
                    t.join()

                for result_docs in results:
                    self._reference_cache.extend(result_docs)

                self._has_more = False

    def count(self, with_limit_and_skip=False):
        """Count the selected elements in the query.

        :param with_limit_and_skip (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count
        """
        if with_limit_and_skip is False:
            return super(QuerySet, self).count(with_limit_and_skip)

        if self._len is None:
            self._len = super(QuerySet, self).count(with_limit_and_skip)

        return self._len

    def enable_parallel_processing(self, chunk_size=200, max_chunks=10):
        new_qs = self.clone()
        new_qs._parallel_processing_enabled = True
        new_qs._parallel_max_chunks = min(max_chunks, MAX_PARALLEL_CHUNKS)
        new_qs._parallel_chunk_size = max(chunk_size, MIN_CHUNK_SIZE)
        return new_qs

    def no_cache(self):
        """Convert to a non_caching queryset

        .. versionadded:: 0.8.3 Convert to non caching queryset
        """
        if self._result_cache is not None:
            raise OperationError("QuerySet already cached")
        return self.clone_into(QuerySetNoCache(self._document, self._collection))


class QuerySetNoCache(BaseQuerySet):
    """A non caching QuerySet"""

    def cache(self):
        """Convert to a caching queryset

        .. versionadded:: 0.8.3 Convert to caching queryset
        """
        return self.clone_into(QuerySet(self._document, self._collection))

    def __repr__(self):
        """Provides the string representation of the QuerySet

        .. versionchanged:: 0.6.13 Now doesnt modify the cursor
        """
        if self._iter:
            return '.. queryset mid-iteration ..'

        data = []
        for i in range(REPR_OUTPUT_SIZE + 1):
            try:
                data.append(next(self))
            except StopIteration:
                break
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."

        self.rewind()
        return repr(data)

    def __iter__(self):
        queryset = self
        if queryset._iter:
            queryset = self.clone()
        queryset.rewind()
        return queryset


class QuerySetNoDeRef(QuerySet):
    """Special no_dereference QuerySet"""

    def __dereference(items, max_depth=1, instance=None, name=None):
        return items
