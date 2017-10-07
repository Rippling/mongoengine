import copy
import operator
import numbers
from collections import Hashable
from functools import partial

import pymongo
from bson import json_util, ObjectId
from bson.dbref import DBRef
from bson.son import SON

from mongoengine import signals
from mongoengine.common import _import_class
from mongoengine.errors import (ValidationError, InvalidDocumentError,
                                LookUpError, FieldDoesNotExist)
from mongoengine.python_support import PY3, txt_type
from mongoengine.base.common import get_document, ALLOW_INHERITANCE
from mongoengine.base.datastructures import (
    BaseDict,
    BaseList,
    EmbeddedDocumentList,
    StrictDict,
    SemiStrictDict
)
from mongoengine.base.fields import ComplexBaseField

from mongoengine.base.proxy import DocumentProxy
from mongoengine.connection import DEFAULT_CONNECTION_NAME

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


class BaseDocument(object):
    _dynamic = False
    _dynamic_lock = True
    STRICT = False

    def __init__(self, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types [ignored]
        :param __only_fields: The fields that have been fetched from DB.
        :param values: A dictionary of values for the document
        """
        super(BaseDocument, self).__init__()
        self._initialised = False
        
        
        # Pop the known values.
        self._created = values.pop("_created", True)
        self._data = values.pop("_data", None)
        
        # Unused
        __auto_convert = values.pop("__auto_convert", True) 
        
        # set default values only to fields loaded from DB
        __only_fields = values.pop("__only_fields", None)
        
        self._python_data = {}
        self._original_values = {}
        self._dynamic_fields = SON()
        self._changed_fields = []
        
        if self._data is None:
            self._data = values
        else:
            self._data = self._translate_db_fields(self._data)

        self._set_default_values(__only_fields)
        
        if "_cls" not in values:
            self._cls = self._class_name

        # Flag initialised
        self._initialised = True
        
    def _translate_db_fields(self, values):
        # On fast-path, mostly we expect this map to be empty.
        db_field_to_name_map = self.__class__._get_db_field_renames()
        for db_field_name, name in db_field_to_name_map.iteritems():
            if db_field_name in values:
                values[name] = values[db_field_name]
                del values[db_field_name]
        return values
        
    def _set_default_values(self, only_fields):
        default_value_fields = self.__class__._get_default_value_fields()
        field_names = set(default_value_fields) - set(self._data)
        
        if only_fields:
            field_names = field_names & set(only_fields)
        
        # On fast-path, we mostly expect fields_names to be empty.
        # defaults mostly will have been saved in DB and will be there in _data.
        for field_name in field_names:
            field = default_value_fields[field_name]
            default = field.default
            if callable(default):
                default = default()
            self._data[field_name] = self._python_data[field_name] = default
            self._changed_fields.append(field_name)
            
    @classmethod
    def _fetch_cls_store(cls, name, fallback):
        key = ('%s_%s' % (name, cls.__name__))
        res = getattr(cls, key, None)
        if res is not None:
            return res
        res = fallback()
        setattr(cls, key, res)
        return res
        
    @classmethod
    def _get_default_value_fields(cls):
        return cls._fetch_cls_store("_default_value_fields", 
            lambda: { field_name : field for field_name, field in cls._fields.iteritems() \
                        if field.default is not None })
        
    @classmethod
    def _get_db_field_renames(cls):
        return cls._fetch_cls_store("_db_field_renames", 
            lambda: { field.db_field: field_name for field_name, field in cls._fields.iteritems() \
                        if field.db_field != field_name })
        

    def __delattr__(self, *args, **kwargs):
        """Handle deletions of fields"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            super(BaseDocument, self).__delattr__(*args, **kwargs)

    def __setattr__(self, name, value):
        try:
            self__created = self._created
        except AttributeError:
            self__created = True

        if (self._is_document and not self__created and
                name in self._meta.get('shard_key', tuple()) and
                self._data.get(name) != value):
            OperationError = _import_class('OperationError')
            msg = "Shard Keys are immutable. Tried to update %s" % name
            raise OperationError(msg)

        try:
            self__initialised = self._initialised
        except AttributeError:
            self__initialised = False
        # Check if the user has created a new instance of a class
        if (self._is_document and self__initialised and
                self__created and name == self._meta.get('id_field')):
            super(BaseDocument, self).__setattr__('_created', False)

        super(BaseDocument, self).__setattr__(name, value)
        
    def __iter__(self):
        return iter(self._fields_ordered)

    def __getitem__(self, name):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            if name in self._fields_ordered:
                return getattr(self, name)
        except AttributeError:
            pass
        raise KeyError(name)

    def __setitem__(self, name, value):
        """Dictionary-style field access, set a field's value.
        """
        # Ensure that the field exists before settings its value
        if not self._dynamic and name not in self._fields:
            raise KeyError(name)
        return setattr(self, name, value)

    def __contains__(self, name):
        try:
            val = getattr(self, name)
            return val is not None
        except AttributeError:
            return False

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        try:
            u = self.__str__()
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        repr_type = str if u is None else type(u)
        return repr_type('<%s: %s>' % (self.__class__.__name__, u))

    def __str__(self):
        if hasattr(self, '__unicode__'):
            if PY3:
                return self.__unicode__()
            else:
                return unicode(self).encode('utf-8')
        return txt_type('%s object' % self.__class__.__name__)

    def __eq__(self, other):
        if type(other) is DocumentProxy:
            return self.id == other.id
        if isinstance(other, self.__class__) and hasattr(other, 'id') and other.id is not None:
            return self.id == other.id
        if isinstance(other, DBRef):
            return self._get_collection_name() == other.collection and self.id == other.id
        if self.id is None:
            return self is other
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if getattr(self, 'pk', None) is None:
            # For new object
            return super(BaseDocument, self).__hash__()
        else:
            return hash(self.pk)
            
    def __copy__(self):
        cls = self.__class__
        res = cls.__new__(cls)
        res.__dict__.update(self.__dict__)
        for k, v in res.__dict__.items():
            if isinstance(v, (dict, list)):
                import copy
                setattr(res, k, copy.copy(v))
        return res

    def clean(self):
        """
        Hook for doing document level data cleaning before validation is run.

        Any ValidationError raised by this method will not be associated with
        a particular field; it will have a special-case association with the
        field defined by NON_FIELD_ERRORS.
        """
        pass

    def get_text_score(self):
        """
        Get text score from text query
        """

        if '_text_score' not in self._data:
            raise InvalidDocumentError('This document is not originally built from a text query')

        return self._data['_text_score']

    def to_mongo(self, use_db_field=True, fields=None):
        """
        Return as SON data ready for use with MongoDB.
        """
        if not fields:
            fields = []

        data = SON()
        data["_id"] = None
        data['_cls'] = self._class_name
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        # only root fields ['test1.a', 'test2'] => ['test1', 'test2']
        root_fields = set([f.split('.')[0] for f in fields])

        for field_name in self:
            if root_fields and field_name not in root_fields:
                continue

            value = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if field is None and self._dynamic:
                field = self._dynamic_fields.get(field_name)

            if value is not None:

                if fields:
                    key = '%s.' % field_name
                    embedded_fields = [
                        i.replace(key, '') for i in fields
                        if i.startswith(key)]

                else:
                    embedded_fields = []

                value = field.to_mongo(value, use_db_field=use_db_field,
                                        fields=embedded_fields)

            # Handle self generating fields
            if value is None and field._auto_gen:
                value = field.generate()
                self._data[field_name] = value

            if value is not None:
                if use_db_field:
                    data[field.db_field] = value
                else:
                    data[field.name] = value

        # If "_id" has not been set, then try and set it
        Document = _import_class("Document")
        if isinstance(self, Document):
            if data["_id"] is None:
                data["_id"] = self._data.get("id", None)

        if data['_id'] is None:
            data.pop('_id')

        # Only add _cls if allow_inheritance is True
        if (not hasattr(self, '_meta') or
                not self._meta.get('allow_inheritance', ALLOW_INHERITANCE)):
            data.pop('_cls')

        return data

    def validate(self, clean=True):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError, error:
                errors[NON_FIELD_ERRORS] = error

        # Get a list of tuples of field names and their current values
        fields = []
        for name in self._fields_ordered:
            field = self._fields.get(name)
            if field is None:
                field = self._dynamic_fields.get(name)
            value = self._python_data.get(name)
            if value is None:
                value = self._data.get(name)
                if value is not None:
                    value = field.to_python(value)
                    self._python_data[name] = value
            
            fields.append((field, value))
                    
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        GenericEmbeddedDocumentField = _import_class(
            "GenericEmbeddedDocumentField")

        for field, value in fields:
            if value is not None:
                try:
                    if isinstance(field, (EmbeddedDocumentField,
                                          GenericEmbeddedDocumentField)):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError, error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError), error:
                    errors[field.name] = error
            elif field.required and not getattr(field, '_auto_gen', False):
                errors[field.name] = ValidationError('Field is required',
                                                     field_name=field.name)

        if errors:
            pk = "None"
            if hasattr(self, 'pk'):
                pk = self.pk
            elif self._instance and hasattr(self._instance, 'pk'):
                pk = self._instance.pk
            message = "ValidationError (%s:%s) " % (self._class_name, pk)
            raise ValidationError(message, errors=errors)

    def to_json(self, *args, **kwargs):
        """Converts a document to JSON.
        :param use_db_field: Set to True by default but enables the output of the json structure with the field names
            and not the mongodb store db_names in case of set to False
        """
        use_db_field = kwargs.pop('use_db_field', True)
        return json_util.dumps(self.to_mongo(use_db_field), *args, **kwargs)

    @classmethod
    def from_json(cls, json_data, created=False):
        """Converts json data to an unsaved document instance"""
        return cls._from_son(json_util.loads(json_data), created=created)

    def __expand_dynamic_values(self, name, value):
        """expand any dynamic values to their correct types / values"""
        if not isinstance(value, (dict, list, tuple)):
            return value

        EmbeddedDocumentListField = _import_class('EmbeddedDocumentListField')

        is_list = False
        if not hasattr(value, 'items'):
            is_list = True
            value = dict([(k, v) for k, v in enumerate(value)])

        if not is_list and '_cls' in value:
            cls = get_document(value['_cls'])
            return cls(**value)

        data = {}
        for k, v in value.items():
            key = name if is_list else k
            data[k] = self.__expand_dynamic_values(key, v)

        if is_list:  # Convert back to a list
            data_items = sorted(data.items(), key=operator.itemgetter(0))
            value = [v for k, v in data_items]
        else:
            value = data

        # Convert lists / values so we can watch for any changes on them
        if (isinstance(value, (list, tuple)) and
                not isinstance(value, BaseList)):
            if issubclass(type(self), EmbeddedDocumentListField):
                value = EmbeddedDocumentList(value, self, name)
            else:
                value = BaseList(value, self, name)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, self, name)

        return value

    def _mark_as_changed(self, key):
        """Marks a key as explicitly changed by the user
        """
        if not key:
            return

        if not hasattr(self, '_changed_fields'):
            return
        
        if not hasattr(self, '_original_values'):
            self._original_values = {}
            
        if '.' in key:
            key, rest = key.split('.', 1)
            key = self._db_field_map.get(key, key)
            key = '%s.%s' % (key, rest)
        else:
            key = self._db_field_map.get(key, key)

        if key not in self._changed_fields:
            levels, idx = key.split('.'), 1
            while idx <= len(levels):
                if '.'.join(levels[:idx]) in self._changed_fields:
                    break
                idx += 1
            else:
                self._changed_fields.append(key)
                if (key not in self._original_values) and ("." not in key):
                    self._original_values[key] = self[key]

                # remove lower level changed fields
                level = '.'.join(levels[:idx]) + '.'
                remove = self._changed_fields.remove
                for field in self._changed_fields:
                    if field.startswith(level):
                        remove(field)

    def _clear_changed_fields(self):
        """Using get_changed_fields iterate and remove any fields that are
        marked as changed"""
        for changed in self._get_changed_fields():
            parts = changed.split(".")
            data = self
            for part in parts:
                if isinstance(data, list):
                    try:
                        data = data[int(part)]
                    except IndexError:
                        data = None
                elif isinstance(data, dict):
                    data = data.get(part, None)
                else:
                    data = getattr(data, part, None)
                if hasattr(data, "_changed_fields"):
                    if hasattr(data, "_is_document") and data._is_document:
                        continue
                    data._changed_fields = []
        self._changed_fields = []
        self._original_values = {}

    def _nestable_types_changed_fields(self, changed_fields, key, data, inspected):
        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, 'items'):
            iterator = enumerate(data)
        else:
            iterator = data.iteritems()

        for index, value in iterator:
            list_key = "%s%s." % (key, index)
            # don't check anything lower if this key is already marked
            # as changed.
            if list_key[:-1] in changed_fields:
                continue
            if hasattr(value, '_get_changed_fields'):
                changed = value._get_changed_fields(inspected)
                changed_fields += ["%s%s" % (list_key, k)
                                   for k in changed if k]
            elif isinstance(value, (list, tuple, dict)):
                self._nestable_types_changed_fields(
                    changed_fields, list_key, value, inspected)

    def _get_changed_fields(self, inspected=None):
        changed_fields = self.__get_changed_fields(inspected)
        changed_fields = sorted(changed_fields, key=len)
        dedup_changed_fields = []
        for field in changed_fields:
            # append a dot at the end to protect cases like ssn and ssnExpectedDate
            isPrefixPresent = next((prefix for prefix in dedup_changed_fields if (prefix + '.') in field), None)
            if not isPrefixPresent:
                dedup_changed_fields.append(field)

        return dedup_changed_fields

    def __get_changed_fields(self, inspected=None):
        """Returns a list of all fields that have explicitly been changed.
        """
        EmbeddedDocument = _import_class("EmbeddedDocument")
        DynamicEmbeddedDocument = _import_class("DynamicEmbeddedDocument")
        ReferenceField = _import_class("ReferenceField")
        SortedListField = _import_class("SortedListField")
        changed_fields = []
        changed_fields += getattr(self, '_changed_fields', [])

        inspected = inspected or set()
        if hasattr(self, 'id') and isinstance(self.id, Hashable):
            if self.id in inspected:
                return changed_fields
            inspected.add(self.id)

        for field_name in self._fields_ordered:
            db_field_name = self._db_field_map.get(field_name, field_name)
            key = '%s.' % db_field_name
            data = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if hasattr(data, 'id'):
                if data.id in inspected:
                    continue
            if isinstance(field, ReferenceField):
                continue
            elif (isinstance(data, (EmbeddedDocument, DynamicEmbeddedDocument))
                  and db_field_name not in changed_fields):
                # Find all embedded fields that have been changed
                changed = data._get_changed_fields(inspected)
                changed_fields += ["%s%s" % (key, k) for k in changed if k]
            elif (isinstance(data, (list, tuple, dict)) and
                    db_field_name not in changed_fields):
                if (hasattr(field, 'field') and
                        isinstance(field.field, ReferenceField)):
                    continue
                elif isinstance(field, SortedListField) and field._ordering:
                    # if ordering is affected whole list is changed
                    if any(map(lambda d: field._ordering in d._changed_fields, data)):
                        changed_fields.append(db_field_name)
                        continue

                self._nestable_types_changed_fields(
                    changed_fields, key, data, inspected)
        return changed_fields

    def _delta(self):
        """Returns the delta (set, unset) of the changes for a document.
        Gets any values that have been explicitly changed.
        """
        # Handles cases where not loaded from_son but has _id
        doc = self.to_mongo()

        set_fields = self._get_changed_fields()
        unset_data = {}
        parts = []
        if hasattr(self, '_changed_fields'):
            set_data = {}
            # Fetch each set item from its path
            for path in set_fields:
                parts = path.split('.')
                d = doc
                new_path = []
                for p in parts:
                    if isinstance(d, (ObjectId, DBRef)):
                        break
                    elif isinstance(d, list) and p.isdigit():
                        try:
                            d = d[int(p)]
                        except IndexError:
                            d = None
                    elif hasattr(d, 'get'):
                        d = d.get(p)
                    new_path.append(p)
                path = '.'.join(new_path)
                set_data[path] = d
        else:
            set_data = doc
            if '_id' in set_data:
                del set_data['_id']

        # Determine if any changed items were actually unset.
        for path, value in set_data.items():
            if value or isinstance(value, (numbers.Number, bool)):
                continue

            # If we've set a value that ain't the default value don't unset it.
            default = None
            if (self._dynamic and len(parts) and parts[0] in
                    self._dynamic_fields):
                del set_data[path]
                unset_data[path] = 1
                continue
            elif path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                parts = path.split('.')
                db_field_name = parts.pop()
                for p in parts:
                    if isinstance(d, list) and p.isdigit():
                        d = d[int(p)]
                    elif (hasattr(d, '__getattribute__') and
                          not isinstance(d, dict)):
                        real_path = d._reverse_db_field_map.get(p, p)
                        d = getattr(d, real_path)
                    else:
                        d = d.get(p)

                if hasattr(d, '_fields'):
                    field_name = d._reverse_db_field_map.get(db_field_name,
                                                             db_field_name)
                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None

            if default is not None:
                if callable(default):
                    default = default()

            if default != value:
                continue

            del set_data[path]
            unset_data[path] = 1
        return set_data, unset_data

    @classmethod
    def _get_collection_name(cls):
        """Returns the collection name for this class. None for abstract class
        """
        return cls._meta.get('collection', None)

    @classmethod
    def _get_db_alias(cls):
        return cls._meta.get('db_alias', DEFAULT_CONNECTION_NAME)

    @classmethod
    def _from_son(cls, son, _auto_dereference=True, only_fields=None, created=False):
        # Return correct subclass for document type
        class_name = son.get('_cls', cls._class_name)
        if class_name != cls._class_name:
            cls = get_document(class_name)
        return cls(_data=son, _created=created, __only_fields=only_fields)

    @classmethod
    def _build_index_specs(cls, meta_indexes):
        """Generate and merge the full index specs
        """
        geo_indices = cls._geo_indices()
        unique_indices = cls._unique_with_indexes()
        base_index_specs = [cls._build_index_spec(spec)
                       for spec in meta_indexes]
        reference_indices = cls._build_reference_indices()
        history_indices = cls._build_history_indices()
        
        # Merge all the indices, so that uniques dont make new indexes.
        res_index_specs = []
        spec_fields = []
        for index_specs in [base_index_specs, reference_indices, geo_indices, unique_indices, history_indices]:
            for index_spec in index_specs:
                index_spec = cls._rippling_process_index_spec(index_spec)
                if not index_spec:
                    continue
                fields = index_spec['fields']
                if fields in spec_fields:
                    res_index_specs[spec_fields.index(fields)].update(index_spec)
                else:
                    res_index_specs.append(index_spec)
                    spec_fields.append(fields)
            
        return res_index_specs
    
    @classmethod
    def _build_reference_indices(cls):
        res = []
        for name, field in cls._fields.iteritems():
            if 'fields.ReferenceField' in str(field):
                res.append({ 'fields': [(name, 1)] })
            if 'fields.CachedReferenceField' in str(field):
                res.append({ 'fields': [("%s._id" % name, 1)] })
        return res
        
    @classmethod
    def _build_history_indices(cls):
        if not cls.__name__.startswith("Historical"):
            return []
        return [{ 'fields': [('_auto_id_0', 1)], 'args': { 'noCompanyPrefix': True } }]
        
    @classmethod
    def _rippling_process_index_spec(cls, spec):
        # Remove `company` for spec['fields']
        spec['fields'] = [field for field in spec['fields'] if field[0] != 'company']
        # Add `company` forcibly to the front.
        noCompanyPrefix = spec.pop('args', {}).get('noCompanyPrefix', False)
        if not noCompanyPrefix and cls._fields.get('company'):
            spec['fields'].insert(0, ('company', 1))
        return spec

    @classmethod
    def _build_index_spec(cls, spec):
        """Build a PyMongo index spec from a MongoEngine index spec.
        """
        if isinstance(spec, basestring):
            spec = {'fields': [spec]}
        elif isinstance(spec, (list, tuple)):
            spec = {'fields': list(spec)}
        elif isinstance(spec, dict):
            spec = dict(spec)

        index_list = []
        direction = None

        # Check to see if we need to include _cls
        allow_inheritance = cls._meta.get('allow_inheritance',
                                          ALLOW_INHERITANCE)
        include_cls = (allow_inheritance and not spec.get('sparse', False) and
                       spec.get('cls',  True) and '_cls' not in spec['fields'])

        # 733: don't include cls if index_cls is False unless there is an explicit cls with the index
        include_cls = include_cls and (spec.get('cls', False) or cls._meta.get('index_cls', True))
        if "cls" in spec:
            spec.pop('cls')
        for key in spec['fields']:
            # If inherited spec continue
            if isinstance(key, (list, tuple)):
                continue

            # ASCENDING from +
            # DESCENDING from -
            # TEXT from $
            # HASHED from #
            # GEOSPHERE from (
            # GEOHAYSTACK from )
            # GEO2D from *
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            elif key.startswith("$"):
                direction = pymongo.TEXT
            elif key.startswith("#"):
                direction = pymongo.HASHED
            elif key.startswith("("):
                direction = pymongo.GEOSPHERE
            elif key.startswith(")"):
                direction = pymongo.GEOHAYSTACK
            elif key.startswith("*"):
                direction = pymongo.GEO2D
            if key.startswith(("+", "-", "*", "$", "#", "(", ")")):
                key = key[1:]

            # Use real field name, do it manually because we need field
            # objects for the next part (list field checking)
            parts = key.split('.')
            if parts in (['pk'], ['id'], ['_id']):
                key = '_id'
            else:
                fields = cls._lookup_field(parts)
                parts = []
                for field in fields:
                    try:
                        if field != "_id":
                            field = field.db_field
                    except AttributeError:
                        pass
                    parts.append(field)
                key = '.'.join(parts)
            index_list.append((key, direction))

        # Don't add cls to a geo index
        if include_cls and direction not in (
                pymongo.GEO2D, pymongo.GEOHAYSTACK, pymongo.GEOSPHERE):
            index_list.insert(0, ('_cls', 1))

        if index_list:
            spec['fields'] = index_list

        return spec

    @classmethod
    def _unique_with_indexes(cls, namespace=""):
        """
        Find and set unique indexes
        """
        unique_indexes = []
        for field_name, field in cls._fields.items():
            sparse = field.sparse
            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, basestring):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with = []
                    for other_name in field.unique_with:
                        parts = other_name.split('.')
                        # Lookup real name
                        parts = cls._lookup_field(parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append('.'.join(name_parts))
                        # Unique field should be required
                        parts[-1].required = True
                        sparse = (not sparse and
                                  parts[-1].name not in cls.__dict__)
                    unique_fields += unique_with

                # Add the new index to the list
                fields = [("%s%s" % (namespace, f), pymongo.ASCENDING)
                          for f in unique_fields]
                index = {'fields': fields, 'unique': True, 'sparse': sparse}
                unique_indexes.append(index)

            if field.__class__.__name__ == "ListField":
                field = field.field

            # Grab any embedded document field unique indexes
            if (field.__class__.__name__ == "EmbeddedDocumentField" and
                    field.document_type != cls):
                field_namespace = "%s." % field_name
                doc_cls = field.document_type
                unique_indexes += doc_cls._unique_with_indexes(field_namespace)

        return unique_indexes

    @classmethod
    def _geo_indices(cls, inspected=None, parent_field=None):
        inspected = inspected or []
        geo_indices = []
        inspected.append(cls)

        geo_field_type_names = ["EmbeddedDocumentField", "GeoPointField",
                                "PointField", "LineStringField", "PolygonField"]

        geo_field_types = tuple([_import_class(field)
                                 for field in geo_field_type_names])

        for field in cls._fields.values():
            if not isinstance(field, geo_field_types):
                continue
            if hasattr(field, 'document_type'):
                field_cls = field.document_type
                if field_cls in inspected:
                    continue
                if hasattr(field_cls, '_geo_indices'):
                    geo_indices += field_cls._geo_indices(
                        inspected, parent_field=field.db_field)
            elif field._geo_index:
                field_name = field.db_field
                if parent_field:
                    field_name = "%s.%s" % (parent_field, field_name)
                geo_indices.append({'fields':
                                    [(field_name, field._geo_index)]})
        return geo_indices

    @classmethod
    def _lookup_field(cls, parts):
        """Lookup a field based on its attribute and return a list containing
        the field's parents and the field.
        """

        ListField = _import_class("ListField")
        DynamicField = _import_class('DynamicField')

        if not isinstance(parts, (list, tuple)):
            parts = [parts]
        fields = []
        field = None

        for field_name in parts:
            # Handle ListField indexing:
            if field_name.isdigit() and isinstance(field, ListField):
                fields.append(field_name)
                continue

            if field is None:
                # Look up first field from the document
                if field_name == 'pk':
                    # Deal with "primary key" alias
                    field_name = cls._meta['id_field']
                if field_name in cls._fields:
                    field = cls._fields[field_name]
                elif cls._dynamic:
                    field = DynamicField(db_field=field_name)
                elif cls._meta.get("allow_inheritance", False) or cls._meta.get("abstract", False):
                    # 744: in case the field is defined in a subclass
                    for subcls in cls.__subclasses__():
                        try:
                            field = subcls._lookup_field([field_name])[0]
                        except LookUpError:
                            continue

                        if field is not None:
                            break
                    else:
                        raise LookUpError('Cannot resolve field "%s"' % field_name)
                else:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
            else:
                ReferenceField = _import_class('ReferenceField')
                GenericReferenceField = _import_class('GenericReferenceField')
                if isinstance(field, (ReferenceField, GenericReferenceField)):
                    raise LookUpError('Cannot perform join in mongoDB: %s' %
                                      '__'.join(parts))
                if hasattr(getattr(field, 'field', None), 'lookup_member'):
                    new_field = field.field.lookup_member(field_name)
                elif cls._dynamic and (isinstance(field, DynamicField) or
                                       getattr(getattr(field, 'document_type', None), '_dynamic', None)):
                    new_field = DynamicField(db_field=field_name)
                else:
                    # Look up subfield on the previous field or raise
                    try:
                        new_field = field.lookup_member(field_name)
                    except AttributeError:
                        raise LookUpError('Cannot resolve subfield or operator {} '
                                          'on the field {}'.format(
                                              field_name, field.name))
                if not new_field and isinstance(field, ComplexBaseField):
                    fields.append(field_name)
                    continue
                elif not new_field:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
                field = new_field  # update field to the new field type
            fields.append(field)
        return fields

    @classmethod
    def _translate_field_name(cls, field, sep='.'):
        """Translate a field attribute name to a database field name.
        """
        parts = field.split(sep)
        parts = [f.db_field for f in cls._lookup_field(parts)]
        return '.'.join(parts)

    def __set_field_display(self):
        """Dynamically set the display value for a field with choices"""
        for attr_name, field in self._fields.items():
            if field.choices:
                if self._dynamic:
                    obj = self
                else:
                    obj = type(self)
                setattr(obj,
                        'get_%s_display' % attr_name,
                        partial(self.__get_field_display, field=field))

    def __get_field_display(self, field):
        """Returns the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            return dict(field.choices).get(value, value)
        return value
