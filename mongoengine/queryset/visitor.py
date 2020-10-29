from __future__ import absolute_import
import copy

from mongoengine.errors import InvalidQueryError
from mongoengine.queryset import transform
from six import iteritems
from six.moves import range

__all__ = ('Q',)


class QNodeVisitor(object):
    """Base visitor class for visiting Q-object nodes in a query tree.
    """

    def visit_combination(self, combination):
        """Called by QCombination objects.
        """
        return combination

    def visit_query(self, query):
        """Called by (New)Q objects.
        """
        return query


class DuplicateQueryConditionsError(InvalidQueryError):
    pass


class SimplificationVisitor(QNodeVisitor):
    """Simplifies query trees by combining unnecessary 'and' connection nodes
    into a single Q-object.
    """

    def visit_combination(self, combination):
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [n.query for n in combination.children]
                try:
                    return Q(**self._query_conjunction(queries))
                except DuplicateQueryConditionsError:
                    # Cannot be simplified
                    pass
        return combination

    def _query_conjunction(self, queries):
        """Merges query dicts - effectively &ing them together.
        """
        query_ops = set()
        combined_query = {}
        for query in queries:
            ops = set(query.keys())
            # Make sure that the same operation isn't applied more than once
            # to a single field
            intersection = ops.intersection(query_ops)
            if intersection:
                raise DuplicateQueryConditionsError()

            query_ops.update(ops)
            from mongoengine import Document
            from mongoengine.base.proxy import DocumentProxy            
            # Convert DocumentProxy to ids.
            def convert_proxy(val):
                if type(val) is DocumentProxy or isinstance(val, Document):
                    return val.id
                return val
                
            for op in ops:
                if isinstance(query[op], list):
                    query[op] = list(map(convert_proxy, query[op]))
                else:
                    query[op] = convert_proxy(query[op)
            
            combined_query.update(copy.deepcopy(query))
            
        return combined_query


class QueryCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, document):
        self.document = document

    def visit_combination(self, combination):
        operator = "$and"
        if combination.operation == combination.OR:
            operator = "$or"
        return {operator: combination.children}

    def visit_query(self, query):
        return transform.query(self.document, **query.query)


class QNode(object):
    """Base class for nodes in query trees.
    """

    AND = 0
    OR = 1

    def to_query(self, document):
        query = self.accept(SimplificationVisitor())
        query = query.accept(QueryCompilerVisitor(document))
        return query

    def accept(self, visitor):
        raise NotImplementedError

    def _combine(self, other, operation):
        """Combine this node with another node into a QCombination object.
        """
        if getattr(other, 'empty', True):
            return self

        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class QCombination(QNode):
    """Represents the combination of several conditions by a given logical
    operator.
    """

    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def accept(self, visitor):
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)

        return visitor.visit_combination(self)

    @property
    def empty(self):
        return not bool(self.children)

    def __repr__(self):
        op = ' & ' if self.operation is self.AND else ' | '
        return '(%s)' % op.join([repr(node) for node in self.children])

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return hash(other) == hash(self)


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def accept(self, visitor):
        return visitor.visit_query(self)

    @property
    def empty(self):
        return not bool(self.query)

    def __repr__(self):
        return 'Q(**%s)' % repr(self.transform_query())

    def transform_query(self):
        def maybe_id(v):
            from mongoengine.base.proxy import DocumentProxy
            from mongoengine import Document
            if type(v) is DocumentProxy or isinstance(v, Document):
                return v.id
            return v

        return {k: maybe_id(v) for k, v in iteritems(self.query)}

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return hash(other) == hash(self)


