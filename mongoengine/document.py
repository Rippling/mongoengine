import warnings
import pymongo
import re

from pymongo.read_preferences import ReadPreference
from bson.dbref import DBRef
from mongoengine import signals
from mongoengine.common import _import_class
from mongoengine.base import (
    DocumentMetaclass,
    TopLevelDocumentMetaclass,
    BaseDocument,
    ALLOW_INHERITANCE,
    get_document
)
from mongoengine.errors import (InvalidQueryError, InvalidDocumentError,
                                SaveConditionError)
from mongoengine.python_support import IS_PYMONGO_3
from mongoengine.queryset import (OperationError, NotUniqueError,
                                  QuerySet, transform)
from mongoengine.connections_manager import connection_manager

import logging

from mongoengine.base.datastructures import WeakInstanceMixin

__all__ = ('Document', 'EmbeddedDocument', 'DynamicDocument',
           'DynamicEmbeddedDocument', 'OperationError', 'InvalidCollectionError',
           'NotUniqueError', 'MapReduceDocument')

_set = object.__setattr__

def includes_cls(fields):
    """ Helper function used for ensuring and comparing indexes
    """

    first_field = None
    if len(fields):
        if isinstance(fields[0], basestring):
            first_field = fields[0]
        elif isinstance(fields[0], (list, tuple)) and len(fields[0]):
            first_field = fields[0][0]
    return first_field == '_cls'

class InvalidCollectionError(Exception):
    pass

class EmbeddedDocument(WeakInstanceMixin, BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.

    A :class:`~mongoengine.EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To disable this behaviour and remove the dependence on the presence of
    `_cls` set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
    dictionary.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

    def __init__(self, *args, **kwargs):
        super(EmbeddedDocument, self).__init__(*args, **kwargs)
        self._changed_fields = {}

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.to_dict() == other.to_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def save(self, *args, **kwargs):
        self._instance.save(*args, **kwargs)

    def reload(self, *args, **kwargs):
        self._instance.reload(*args, **kwargs)


class Document(BaseDocument):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To disable this behaviour and remove the dependence on the presence of
    `_cls` set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
    dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. :attr:`max_size` is rounded up
    to the next multiple of 256 by MongoDB internally and mongoengine before.
    Use also a multiple of 256 to avoid confusions.  If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10485760 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    :attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a
    deployment system.

    By default, _cls will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting cls to False on the specific index or
    by setting index_cls to False on the meta dictionary for the document.

    By default, any extra attribute existing in stored data but not declared
    in your model will raise a :class:`~mongoengine.FieldDoesNotExist` error.
    This can be disabled by setting :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    def pk():
        """Primary key alias
        """

        def fget(self):
            return getattr(self, self._meta['id_field'])

        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)

        return property(fget, fset)

    pk = pk()

    def modify(self, query={}, **update):
        """Perform an atomic update of the document in the database and reload
        the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn't match the query.

        .. note:: All unsaved changes that have been made to the document are
            rejected if the method returns True.

        :param query: the update will be performed only if the document in the
            database matches the query
        :param update: Django-style update keyword arguments
        """

        if self.pk is None:
            raise InvalidDocumentError("The document does not have a primary key.")

        id_field = self._meta["id_field"]
        query = query.copy() if isinstance(query, dict) else query.to_query(self)

        if id_field in query and query[id_field] != self.pk:
            raise InvalidQueryError("Invalid document modify query: it must modify only this document.")

        qs = self._qs(__raw__=self._db_object_key)
        if query:
            qs = qs.filter(**query)
        updated = qs.modify(new=True, **update)
        if updated is None:
            return False

        _set(self, '_db_data', updated._db_data)
        _set(self, '_internal_data', {})
        _set(self, '_lazy', False)
        self._clear_changed_fields()

        return True


    def save(self, validate=True, clean=True,
             write_concern=None, cascade=None, cascade_kwargs=None,
             _refs=None, save_condition=None, signal_kwargs=None,
             alias=None, collection_name=None, full=False, **kwargs):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :parm signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        .. versionchanged:: 0.8.5
            Optional save_condition that only overwrites existing documents
            if the condition is satisfied in the current db record.
        .. versionchanged:: 0.10
            :class:`OperationError` exception raised if save_condition fails.
        .. versionchanged:: 0.10.1
            :class: save_condition failure now raises a `SaveConditionError`
        .. versionchanged:: 0.10.7
            Add signal_kwargs argument
        """
        signal_kwargs = signal_kwargs or {}
        signals.pre_save.send(self.__class__, document=self, **signal_kwargs)

        if validate:
            self.validate(clean=clean)

        if write_concern is None:
            write_concern = {"w": 1}

        try:
            collection = connection_manager.get_and_setup(self.__class__, alias=alias, collection_name=collection_name)
            if self._created:
                # Update: Get delta.
                sets, unsets = self._delta(full)
                db_id_field = self._fields[self._meta['id_field']].db_field
                sets.pop(db_id_field, None)

                update_query = {}
                if sets:
                    update_query['$set'] = sets
                if unsets:
                    update_query['$unset'] = unsets

                if update_query:
                    collection.update(self._db_object_key, update_query, **write_concern)

                created = False
            else:
                # Insert: Get full SON.
                doc = self.to_mongo()
                object_id = collection.insert(doc, **write_concern)
                # Fix pymongo's "return return_one and ids[0] or ids":
                # If the ID is 0, pymongo wraps it in a list.
                if isinstance(object_id, list) and not object_id[0]:
                    object_id = object_id[0]

                id_field = self._meta['id_field']
                del self._internal_data[id_field]
                _set(self, '_db_data', doc)
                doc['_id'] = object_id

                created = True

            cascade = (self._meta.get('cascade', False)
                       if cascade is None else cascade)
            if cascade:
                kwargs = {
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                self.cascade_save(**kwargs)

            cascade = (self._meta.get('cascade', False)
                       if cascade is None else cascade)
            if cascade:
                kwargs = {
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                self.cascade_save(**kwargs)
        except pymongo.errors.DuplicateKeyError, err:
            message = u'Tried to save duplicate unique keys (%s)'
            raise NotUniqueError(message % unicode(err))
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', unicode(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % unicode(err))
            raise OperationError(message % unicode(err))
        self._clear_changed_fields()
        signals.post_save.send(self.__class__, document=self,
                               created=created, **signal_kwargs)
        return self

    def cascade_save(self, *args, **kwargs):
        """Recursively saves any references /
           generic references on an objects"""
        _refs = kwargs.get('_refs', []) or []

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')

        for name, cls in self._fields.items():
            if not isinstance(cls, (ReferenceField,
                                    GenericReferenceField)):
                continue

            ref = getattr(self, name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, '_changed_fields', True):
                continue

            if getattr(ref, '_lazy', False):
                continue

            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref.to_dict()))
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    @property
    def _qs(self):
        """
        Returns the queryset to use for updating / reloading / deletions
        """
        if not hasattr(self, '__objects'):
            self.__objects = QuerySet(self, connection_manager.get_and_setup(self.__class__))
        return self.__objects

    @property
    def _object_key(self):
        """Dict to identify object in collection
        """
        select_dict = {'pk': self.pk}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            select_dict[k] = getattr(self, k)
        return select_dict

    @property
    def _db_object_key(self):
        field = self._fields[self._meta['id_field']]
        select_dict = {field.db_field: field.to_mongo(self.pk)}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            # For a lazy instance of a reference field, we want to fetch the
            # entire object so we can properly perform operations that require
            # the entire shard key (such as findAndModify). The reload method
            # is aware of lazy objects.
            if self._lazy and k != self._meta['id_field']:
                self.reload()
            actual_key = self._db_field_map.get(k, k)
            select_dict[actual_key] = self._fields[k].to_mongo(getattr(self, k))
        return select_dict

    def update(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        if not self.pk:
            raise OperationError('attempt to update a document not yet saved')

        # Need to add shard key to query, or you get an error
        return self._qs.filter(**self._object_key).update_one(**kwargs)

    def delete(self, **write_concern):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """
        signals.pre_delete.send(self.__class__, document=self)

        if not write_concern:
            write_concern = {'w': 1}

        try:
            self._qs.filter(**self._object_key).delete(write_concern=write_concern, _from_doc_delete=True)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)
        signals.post_delete.send(self.__class__, document=self)

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.

        .. versionadded:: 0.5
        """
        DeReference = _import_class('DeReference')
        DeReference()([self], max_depth + 1)
        return self

    def reload(self):
        """Reloads all attributes from the database.
        """
        id_field = self._meta['id_field']
        collection = self._get_collection()
        # If this is a lazy object, we only have the ID field and don't want to
        # call _db_object_key, since _db_object_key could fetch (reload) the
        # object.
        if self._lazy:
            son = collection.find_one({ '_id': self.pk })
        else:
            son = collection.find_one(self._db_object_key)
        if son == None:
            raise self.DoesNotExist('Document has been deleted.')
        _set(self, '_db_data', son)
        _set(self, '_internal_data', {})
        _set(self, '_lazy', False)
        self._clear_changed_fields()
        return self

    def to_dbref(self):
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if not self.pk:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        classes = [get_document(class_name)
                   for class_name in cls._subclasses
                   if class_name != cls.__name__] + [cls]
        documents = [get_document(class_name)
                     for class_name in document_cls._subclasses
                     if class_name != document_cls.__name__] + [document_cls]

        for klass in classes:
            for document_cls in documents:
                delete_rules = klass._meta.get('delete_rules') or {}
                delete_rules[(document_cls, field_name)] = rule
                klass._meta['delete_rules'] = delete_rules

    @classmethod
    def drop_collection(cls, alias=None, collection_name=None):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.

        Raises :class:`OperationError` if the document has no collection set
        (i.g. if it is `abstract`)

        .. versionchanged:: 0.10.7
            :class:`OperationError` exception raised if no collection available
        """
        connection_manager.drop_collection(cls, alias=alias, collection_name=collection_name)

    @classmethod
    def _get_collection(cls, alias=None, collection_name=None):
        return connection_manager.get_and_setup(cls, alias=alias, collection_name=collection_name)

    @classmethod
    def _get_db(cls, alias=None):
        if alias is None:
            alias = cls._get_db_alias()
        return connection_manager._get_db(alias)
        
    @classmethod
    def __create_index(cls, *args, **kwargs):
        collection = connection_manager.get_collection(cls)
        try:
            collection.create_index(*args, **kwargs)
        except Exception as e:
            if str(e.__class__) == "OperationFailure" and hasattr(e, 'message'):
                m = re.match("Index with name: (.*) already exists with different options", e.message)
                if m:
                    indexName = m.group(1)
                    logging.warning("Dropping index: %s on %s due to diff index options", indexName, collection.name)
                    connection_manager.get_collection(cls).drop_index(indexName)
                    connection_manager.get_collection(cls).create_index(*args, **kwargs)
                else:
                    raise
            

    @classmethod
    def create_index(cls, keys, background=False, **kwargs):
        """Creates the given indexes if required.

        :param keys: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        :param background: Allows index creation in the background
        """
        index_spec = cls._build_index_spec(keys)
        index_spec = index_spec.copy()
        fields = index_spec.pop('fields')
        drop_dups = kwargs.get('drop_dups', False)
        if IS_PYMONGO_3 and drop_dups:
            msg = "drop_dups is deprecated and is removed when using PyMongo 3+."
            warnings.warn(msg, DeprecationWarning)
        elif not IS_PYMONGO_3:
            index_spec['drop_dups'] = drop_dups
        index_spec['background'] = True # background
        index_spec.update(kwargs)

        if IS_PYMONGO_3:
            return cls.__create_index(fields, **index_spec)
        else:
            return connection_manager.get_collection(cls).ensure_index(fields, **index_spec)

    @classmethod
    def ensure_index(cls, key_or_list, drop_dups=False, background=False,
                     **kwargs):
        """Ensure that the given indexes are in place. Deprecated in favour
        of create_index.

        :param key_or_list: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        :param background: Allows index creation in the background
        :param drop_dups: Was removed/ignored with MongoDB >2.7.5. The value
            will be removed if PyMongo3+ is used
        """
        if IS_PYMONGO_3 and drop_dups:
            msg = "drop_dups is deprecated and is removed when using PyMongo 3+."
            warnings.warn(msg, DeprecationWarning)
        elif not IS_PYMONGO_3:
            kwargs.update({'drop_dups': drop_dups})
        return cls.create_index(key_or_list, background=background, **kwargs)

    @classmethod
    def ensure_indexes(cls, collection):
        """Checks the document meta data and ensures all the indexes exist.

        Global defaults can be set in the meta - see :doc:`guide/defining-documents`

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        background = cls._meta.get('index_background', False)
        drop_dups = cls._meta.get('index_drop_dups', False)
        index_opts = cls._meta.get('index_opts') or {}
        index_cls = cls._meta.get('index_cls', True)
        if IS_PYMONGO_3 and drop_dups:
            msg = "drop_dups is deprecated and is removed when using PyMongo 3+."
            warnings.warn(msg, DeprecationWarning)

        # 746: when connection is via mongos, the read preference is not necessarily an indication that
        # this code runs on a secondary
        if not collection.is_mongos and collection.read_preference > 1:
            return

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed = False

        # Ensure document-defined indexes are created
        if cls._meta['index_specs']:
            index_spec = cls._meta['index_specs']
            for spec in index_spec:
                spec = spec.copy()
                fields = spec.pop('fields')
                cls_indexed = cls_indexed or includes_cls(fields)
                opts = index_opts.copy()
                opts.update(spec)

                # we shouldn't pass 'cls' to the collection.ensureIndex options
                # because of https://jira.mongodb.org/browse/SERVER-769
                if 'cls' in opts:
                    del opts['cls']

                if IS_PYMONGO_3:
                    cls.__create_index(fields, background=background, **opts)
                else:
                    collection.ensure_index(fields, background=background,
                                            drop_dups=drop_dups, **opts)

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if (index_cls and not cls_indexed and
                cls._meta.get('allow_inheritance', ALLOW_INHERITANCE) is True):

            # we shouldn't pass 'cls' to the collection.ensureIndex options
            # because of https://jira.mongodb.org/browse/SERVER-769
            if 'cls' in index_opts:
                del index_opts['cls']

            if IS_PYMONGO_3:
                collection.create_index('_cls', background=background,
                                        **index_opts)
            else:
                collection.ensure_index('_cls', background=background,
                                        **index_opts)

    @classmethod
    def list_indexes(cls):
        """ Lists all of the indexes that should be created for given
        collection. It includes all the indexes from super- and sub-classes.
        """

        if cls._meta.get('abstract'):
            return []

        # get all the base classes, subclasses and siblings
        classes = []

        def get_classes(cls):

            if (cls not in classes and
                    isinstance(cls, TopLevelDocumentMetaclass)):
                classes.append(cls)

            for base_cls in cls.__bases__:
                if (isinstance(base_cls, TopLevelDocumentMetaclass) and
                        base_cls != Document and
                        not base_cls._meta.get('abstract') and
                        connection_manager.get_collection(base_cls).full_name == connection_manager.get_collection(cls).full_name and
                        base_cls not in classes):
                    classes.append(base_cls)
                    get_classes(base_cls)
            for subclass in cls.__subclasses__():
                if (isinstance(base_cls, TopLevelDocumentMetaclass) and
                        connection_manager.get_collection(subclass).full_name == connection_manager.get_collection(cls).full_name and
                        subclass not in classes):
                    classes.append(subclass)
                    get_classes(subclass)

        get_classes(cls)

        # get the indexes spec for all of the gathered classes
        def get_indexes_spec(cls):
            indexes = []

            if cls._meta['index_specs']:
                index_spec = cls._meta['index_specs']
                for spec in index_spec:
                    spec = spec.copy()
                    fields = spec.pop('fields')
                    indexes.append(fields)
            return indexes

        indexes = []
        for klass in classes:
            for index in get_indexes_spec(klass):
                if index not in indexes:
                    indexes.append(index)

        # finish up by appending { '_id': 1 } and { '_cls': 1 }, if needed
        if [(u'_id', 1)] not in indexes:
            indexes.append([(u'_id', 1)])
        if (cls._meta.get('index_cls', True) and
                cls._meta.get('allow_inheritance', ALLOW_INHERITANCE) is True):
            indexes.append([(u'_cls', 1)])

        return indexes

    @classmethod
    def compare_indexes(cls):
        """ Compares the indexes defined in MongoEngine with the ones existing
        in the database. Returns any missing/extra indexes.
        """

        required = cls.list_indexes()
        existing = [info['key']
                    for info in connection_manager.get_collection(cls).index_information().values()]
        missing = [index for index in required if index not in existing]
        extra = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [(u'_cls', 1)] in missing:
            cls_obsolete = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([(u'_cls', 1)])

        return {'missing': missing, 'extra': extra}


class DynamicDocument(Document):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expando style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.fields.DynamicField` and data can be attributed to that
    field.

    .. note::

        There is one caveat on Dynamic Documents: fields cannot start with `_`
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        if field_name in self._dynamic_fields:
            setattr(self, field_name, None)
        else:
            super(DynamicDocument, self).__delattr__(*args, **kwargs)


class DynamicEmbeddedDocument(EmbeddedDocument):
    """A Dynamic Embedded Document class allowing flexible, expandable and
    uncontrolled schemas. See :class:`~mongoengine.DynamicDocument` for more
    information about dynamic documents.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            setattr(self, field_name, None)


class MapReduceDocument(object):
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.

    .. versionadded:: 0.3
    """

    def __init__(self, document, collection, key, value):
        self._document = document
        self._collection = collection
        self.key = key
        self.value = value

    @property
    def object(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta['id_field']
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except Exception:
                raise Exception("Could not cast key as %s" %
                                id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object
