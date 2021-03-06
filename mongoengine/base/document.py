from __future__ import absolute_import
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
from mongoengine.common import ReadOnlyContext

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
from six import string_types, iteritems, text_type

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


class BaseDocument(object):
    _dynamic = False
    _dynamic_lock = True
    STRICT = False

    def __init__(self, *args, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types
        :param values: A dictionary of values for the document
        """
        super(BaseDocument, self).__init__()
        self._initialised = False
        self._created = True
        
        kwargs_passed = values.pop("kwargs_passed", None)
        loading_from_db = values.pop("loading_from_db", False)
        if kwargs_passed is not None:
            values.update(kwargs_passed)
            
        if args:
            # Combine positional arguments with named arguments.
            # We only want named arguments.
            field = iter(self._fields_ordered)
            # If its an automatic id field then skip to the first defined field
            if getattr(self, '_auto_id_field', False):
                next(field)
            for value in args:
                name = next(field)
                if name in values:
                    raise TypeError(
                        "Multiple values for keyword argument '" + name + "'")
                values[name] = value

        __auto_convert = values.pop("__auto_convert", True)

        # 399: set default values only to fields loaded from DB
        __only_fields = values.pop("__only_fields", None)
        self._only_fields = __only_fields
        if self._only_fields:
            self._only_fields = set(self._only_fields)
            self._only_fields.add("_id")

        _created = values.pop("_created", True)

        signals.pre_init.send(self.__class__, document=self, values=values)

        self._data = {}

        if "_cls" not in values:
            self._cls = self._class_name
        
        # Set passed values after initialisation
        FileField = _import_class('FileField')
        for key, value in iteritems(values):
            if key == '__auto_convert':
                continue
            key = self._reverse_db_field_map.get(key, key)
            if key in self._fields or key in ('id', 'pk', '_cls'):
                field = self._fields.get(key)
                optimize_v2 = field and field.is_v2_field() and loading_from_db
                if __auto_convert and value is not None:
                    if field and not isinstance(field, FileField) and not optimize_v2:
                        value = field.to_python(value)
                if optimize_v2:
                    self._data[key] = value
                else:
                    self.setattr_quick(key, value)
            else:
                self._data[key] = value
                    
        # Set the default values.
        for key, field in iteritems(self._fields):
            if key in self._data or field.default is None:
                # If the data exists or no defaults exist
                # We don't need to set the defaults.
                continue
            if __only_fields and self._db_field_map.get(key, key) not in __only_fields:
                # If only certain fields were loaded, we do not set the defaults for the 
                # fields that aren't loaded.
                continue
            value = getattr(self, key, None)
            setattr(self, key, value)

        # Set any get_fieldname_display methods. Rippling doesn't require this, so commenting it out.
        # self.__set_field_display()

        # Flag initialised
        self._initialised = True
        self._created = _created
        signals.post_init.send(self.__class__, document=self)
        
        # Unused:
        self._dynamic_fields = SON()

    def get_python_data(self):
        if "_python_data" not in self.__dict__:
            self._python_data = {}
        return self._python_data
        
    def v2_get(self, field):
        pd = self.get_python_data()
        if field.name in pd:
            return pd[field.name]
        value = self._data.get(field.name)
        value = field.to_python(value)
        pd[field.name] = value
        return value
        
    def v2_get_by_name(self, name):
        return self.v2_get(self._meta.get_field(name))

    def v2_set(self, field, value):
        pd = self.get_python_data()
        self._data[field.name] = pd[field.name] = value

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
        # Handle dynamic data only if an initialised dynamic document
        if self._dynamic and not self._dynamic_lock:

            if not hasattr(self, name) and not name.startswith('_'):
                DynamicField = _import_class("DynamicField")
                field = DynamicField(db_field=name)
                field.name = name
                self._dynamic_fields[name] = field
                self._fields_ordered += (name,)

            if not name.startswith('_'):
                value = self.__expand_dynamic_values(name, value)

            # Handle marking data as changed
            if name in self._dynamic_fields:
                self._data[name] = value
                if hasattr(self, '_changed_fields'):
                    self._mark_as_changed(name)
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
        
    
    
    def setattr_quick(self, name, value):
        super(BaseDocument, self).__setattr__(name, value)

    def __getstate__(self):
        data = {}
        for k in ('_changed_fields', '_initialised', '_created',
                  '_dynamic_fields', '_fields_ordered', '_only_fields'):
            if hasattr(self, k):
                data[k] = getattr(self, k)
        data['_data'] = self.to_mongo()
        return data

    def __setstate__(self, data):
        if isinstance(data["_data"], SON):
            data["_data"] = self.__class__._from_son(data["_data"])._data
        for k in ('_changed_fields', '_initialised', '_created', '_data',
                  '_dynamic_fields', '_only_fields'):
            if k in data:
                setattr(self, k, data[k])
        if '_fields_ordered' in data:
            if self._dynamic:
                setattr(self, '_fields_ordered', data['_fields_ordered'])
            else:
                _super_fields_ordered = type(self)._fields_ordered
                setattr(self, '_fields_ordered', _super_fields_ordered)

        dynamic_fields = data.get('_dynamic_fields') or SON()
        for k in dynamic_fields.keys():
            setattr(self, k, data["_data"].get(k))

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
                return text_type(self).encode('utf-8')
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

    def to_mongo(self, use_db_field=True, fields=None, serial_v2=False):
        """
        Return as SON data ready for use with MongoDB.
        """
        if not fields:
            fields = []

        data = {} if serial_v2 else SON()
        data["_id"] = None
        data['_cls'] = self._class_name
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        # only root fields ['test1.a', 'test2'] => ['test1', 'test2']
        root_fields = set([f.split('.')[0] for f in fields])

        for field_name in self:
            field = self._fields.get(field_name)
            output_field_name = field.db_field if use_db_field else field.name
            if root_fields and output_field_name not in root_fields:
                continue

            if field and field.is_v2_field():
                value = self.v2_get(field)
            else:
                value = self._data.get(field_name, None)

            if value is not None:

                if fields:
                    key = '%s.' % output_field_name
                    embedded_fields = [
                        i[len(key):] for i in fields
                        if i.startswith(key)]

                else:
                    embedded_fields = []

                if serial_v2 and isinstance(field, EmbeddedDocumentField):
                    # `serial_v2` is only used by `RPOptimizedSerializer` which re-serializes embedded document fields anyway
                    # any data we assign to this field will be replaced
                    value = None
                else:
                    value = field.to_mongo(value, use_db_field=use_db_field,
                                           fields=embedded_fields, serial_v2=serial_v2)

            # Handle self generating fields
            if value is None:
                if field._auto_gen:
                    value = field.generate()
                    self._data[field_name] = value
                elif serial_v2:
                    data[output_field_name] = None

            if value is not None:
                data[output_field_name] = value

        # If "_id" has not been set, then try and set it
        Document = _import_class("Document")
        if isinstance(self, Document):
            if data["_id"] is None:
                data["_id"] = self._data.get("id", None)
                if data["_id"] and serial_v2:
                    data["_id"] = str(data["_id"])

        if data['_id'] is None:
            data.pop('_id')

        # Only add _cls if allow_inheritance is True
        if (not hasattr(self, '_meta') or
                not self._meta.get('allow_inheritance', ALLOW_INHERITANCE)):
            data.pop('_cls')

        return data

    def validate(self, clean=True, **kwargs):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                # Condition added so that we do not have to  change implementation of all clean methods
                # The clean methods which need kwargs can update the method signature.
                if kwargs:
                    self.clean(**kwargs)
                else:
                    self.clean()
            except ValidationError as error:
                errors[NON_FIELD_ERRORS] = error

        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        GenericEmbeddedDocumentField = _import_class(
            "GenericEmbeddedDocumentField")
        ComplexBaseField = _import_class("ComplexBaseField")

        for field_name in self._fields_ordered:
            field = self._fields.get(field_name, None)
            if field and field.is_v2_field():
                value = self.v2_get(field)
            else:
                value = self._data.get(field_name)
            if value is not None:
                try:
                    if isinstance(field, (EmbeddedDocumentField,
                                          GenericEmbeddedDocumentField,
                                          ComplexBaseField)):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError) as error:
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
            data_items = sorted(list(data.items()), key=operator.itemgetter(0))
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

    def force_mark_as_changed(self, key):
        self._mark_as_changed(key)
        self._force_changed_fields = getattr(self, '_force_changed_fields', set())
        self._force_changed_fields.add(key)

    def _mark_as_changed(self, key):
        """Marks a key as explicitly changed by the user
        """
        if not key:
            return

        if not hasattr(self, '_changed_fields'):
            return
        
        if not hasattr(self, '_original_values'):
            self._original_values = {}

        model_key = key
        if '.' in key:
            key, rest = key.split('.', 1)
            key = self._db_field_map.get(key, key)
            key = '%s.%s' % (key, rest)
        else:
            key = self._db_field_map.get(key, key)

        if key not in self._changed_fields:
            try:
                if getattr(self, '_instance', None) and getattr(self, "_root_field_name", None):
                    self._instance._mark_as_changed(self._root_field_name)
            except:
                # Ignore if the weakreference has been cleaned up
                pass

            levels, idx = key.split('.'), 1
            while idx <= len(levels):
                if '.'.join(levels[:idx]) in self._changed_fields:
                    break
                idx += 1
            else:
                self._changed_fields.append(key)
                if (key not in self._original_values) and ("." not in key):
                    # deepcopy for lists, embedded docs etc. This should not be a costly operation since we are doing
                    # this only when the field changes. This is useful to kill on_changes when
                    # Original value of self.field was ['x'], and there were multiple assignments
                    # self.field = []
                    # self.field.append('x')
                    # The on_change should not be fired even in this case.
                    with DocumentProxy.ignore_deep_copy():
                        # Always use model_key names here as this will give KeyError for db_field names.
                        self._original_values[key] = copy.deepcopy(self[model_key])

                # remove lower level changed fields
                level = '.'.join(levels[:idx]) + '.'
                remove = self._changed_fields.remove
                for field in self._changed_fields:
                    if field.startswith(level):
                        remove(field)

    def _clear_changed_fields(self):
        """Using get_changed_fields iterate and remove any fields that are
        marked as changed"""
        ReferenceField = _import_class("ReferenceField")
        for changed in self._get_changed_fields():
            parts = changed.split(".")
            data = self
            for part in parts:
                field = data._fields.get(part, None) if hasattr(data, '_fields') and data._fields else None
                if isinstance(data, list):
                    try:
                        data = data[int(part)]
                    except IndexError:
                        data = None
                elif isinstance(data, dict):
                    data = data.get(part, None)
                else:
                    data = getattr(data, part, None)

                if (field and isinstance(field, ReferenceField)) or (hasattr(data, "_is_document") and data._is_document):
                    continue
                if hasattr(data, '_changed_fields'):
                    data._changed_fields = []
                if hasattr(data, '__clear_changed_field'):
                    data.__clear_changed_field()

                elif isinstance(data, (list, tuple, dict)):
                    # if field inside the embedded document is reference field then skip
                    if field and hasattr(field, 'field') and isinstance(field.field, ReferenceField):
                        continue
                    self.__clear_nested_types_changed_field(data)


        self._changed_fields = []
        self._original_values = {}
        self._force_changed_fields = set()

    def __clear_changed_field(self):
        """
        recursively clears the changed field of all the fields of `data`
        we can directly clear the `changed fields` without looking at whether field has actually changed since we are
        clearing `changed fields` of top level document
        """
        EmbeddedDocument = _import_class("EmbeddedDocument")
        DynamicEmbeddedDocument = _import_class("DynamicEmbeddedDocument")
        ReferenceField = _import_class("ReferenceField")
        for field_name in self._fields_ordered:
            data = self._data.get(field_name, None)
            field = self._fields.get(field_name)
            if isinstance(field, ReferenceField) or (hasattr(data, '_is_document') and data._is_document):
                continue
            if isinstance(data, (list, tuple, dict)):
                # if field inside the embedded document is reference field then skip
                if hasattr(field, 'field') and isinstance(field.field, ReferenceField):
                    continue
                self.__clear_nested_types_changed_field(data)

            elif isinstance(data, (EmbeddedDocument, DynamicEmbeddedDocument)):
                data.__clear_changed_field()

            if hasattr(data, "_changed_fields"):
                data._changed_fields = []

        if hasattr(self, "_changed_fields") and not(hasattr(self, '_is_document') and self._is_document):
            self._changed_fields = []

    def __clear_nested_types_changed_field(self, data):
        ReferenceField = _import_class("ReferenceField")
        if not hasattr(data, 'items'):
            iterator = enumerate(data)
        else:
            iterator = iteritems(data)

        for index, value in iterator:
            if hasattr(value, '_is_document') and value._is_document:
                continue
            if hasattr(value, "__clear_changed_field"):
                value.__clear_changed_field()

            elif isinstance(value, (list, tuple, dict)):
                self.__clear_nested_types_changed_field(value)

            if hasattr(value, "_changed_fields"):
                value._changed_fields = []

    def _nestable_types_changed_fields(self, changed_fields, key, data, inspected):
        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, 'items'):
            iterator = enumerate(data)
        else:
            iterator = iteritems(data)

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
        changed_fields = getattr(self, '_changed_fields', [])
        original_values = getattr(self, '_original_values', {})

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
                for k in changed:
                    if k:
                        field_name = "%s%s" % (key, k)
                        if field_name not in changed_fields:
                            changed_fields.append(field_name)
                            if k in getattr(data, '_original_values', {}):
                                original_values[field_name] = getattr(data, '_original_values', {})[k]
            elif (isinstance(data, (list, tuple, dict)) and
                    db_field_name not in changed_fields):
                if (hasattr(field, 'field') and
                        isinstance(field.field, ReferenceField)):
                    continue
                elif isinstance(field, SortedListField) and field._ordering:
                    # if ordering is affected whole list is changed
                    if any([field._ordering in d._changed_fields for d in data]):
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
            if value or isinstance(value, (numbers.Number, bool, list)):
                continue

            # If we've set a value that ain't the default value don't unset it.
            default = None
            to_unset = True
            if (self._dynamic and len(parts) and parts[0] in
                    self._dynamic_fields):
                del set_data[path]
                unset_data[path] = 1
                continue
            elif path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                d_type = type(self)
                parts = path.split('.')
                db_field_name = parts.pop()
                for p in parts:
                    if isinstance(d, list) and p.isdigit():
                        d_next = d[int(p)]
                    elif (hasattr(d, '__getattribute__') and
                          not isinstance(d, dict)):
                        real_path = d._reverse_db_field_map.get(p, p)
                        d_next = getattr(d, real_path)
                    else:
                        d_next = d.get(p)
                    if hasattr(d, '_fields') and p in d._fields:
                        d_type = d._fields[p]
                    else:
                        d_type = d_next
                    d = d_next
                from mongoengine import DictField
                if hasattr(d, '_fields'):
                    field_name = d._reverse_db_field_map.get(db_field_name,
                                                             db_field_name)
                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None
                elif isinstance(d_type, DictField):
                    to_unset = False

            if default is not None:
                if callable(default):
                    default = default()
            if not to_unset or default != value:
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
        from mongoengine.connection import aliases
        alias = DEFAULT_CONNECTION_NAME
        app_name = cls.__module__.split(".")[-2]
        if app_name in aliases:
            alias = app_name
        class_alias = cls._meta.get('db_alias', None)
        if class_alias is not None and class_alias in aliases:
            alias = class_alias
        if ReadOnlyContext.isActive():
            alias += '_read_only'
        return alias

    @classmethod
    def _from_son(cls, son, _auto_dereference=True, only_fields=None, created=False, _lazy_prefetch_base=None,
                  _fields=None, loading_from_db=True):
        """Create an instance of a Document (subclass) from a PyMongo SON.
        """
        if not only_fields:
            only_fields = []

        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get('_cls', cls._class_name)
        data = dict(("%s" % key, value) for key, value in iteritems(son))

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        changed_fields = []
        errors_dict = {}

        fields = cls._fields
        if not _auto_dereference:
            fields = copy.copy(fields)

        ReferenceField = _import_class("ReferenceField")
        CachedReferenceField = _import_class("CachedReferenceField")
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        ListField = _import_class("ListField")

        for field_name, field in iteritems(fields):
            field._auto_dereference = _auto_dereference
            if field.db_field in data:
                value = data[field.db_field]
                try:
                    if value is not None:
                        if not field.is_v2_field():
                            # Pass queryset for ReferenceFields only
                            has_reference = isinstance(field, (ReferenceField, CachedReferenceField, EmbeddedDocumentField))
                            if has_reference:
                                # _fields contains a stack of reference fields being fetched.
                                # e.g. D.ref1.ref2.ref3 -> [ref1, ref2, ref3]
                                _fields is not None and _fields.append(field)
                                value = field.to_python(value, _lazy_prefetch_base=_lazy_prefetch_base, _fields=_fields,
                                                        loading_from_db=loading_from_db)
                                _fields is not None and _fields.pop()
                            else:
                                if isinstance(field, ListField):
                                    value = field.to_python(value, loading_from_db=loading_from_db)
                                else:
                                    value = field.to_python(value)

                    data[field_name] = value
                    if field_name != field.db_field:
                        del data[field.db_field]
                except (AttributeError, ValueError) as e:
                    errors_dict[field_name] = e
            elif field.default:
                default = field.default
                if callable(default):
                    default = default()
                if isinstance(default, BaseDocument):
                    changed_fields.append(field_name)
                elif not only_fields or field_name in only_fields:
                    changed_fields.append(field_name)

        if errors_dict:
            errors = "\n".join(["%s - %s" % (k, v)
                                for k, v in errors_dict.items()])
            msg = ("Invalid data to create a `%s` instance.\n%s"
                   % (cls._class_name, errors))
            raise InvalidDocumentError(msg)

        if cls.STRICT:
            data = dict((k, v)
                        for k, v in iteritems(data) if k in cls._fields)
        obj = cls(__auto_convert=False, _created=created, __only_fields=only_fields, kwargs_passed=data,
                  loading_from_db=loading_from_db)
        obj._changed_fields = changed_fields
        if not _auto_dereference:
            obj._fields = fields

        return obj

    @classmethod
    def _build_index_specs(cls, meta_indexes):
        """Generate and merge the full index specs
        """
        geo_indices = cls._geo_indices()
        unique_indices = cls._unique_with_indexes()
        base_index_specs = [cls._build_index_spec(spec) for spec in meta_indexes]
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
        for name, field in iteritems(cls._fields):
            if 'fields.ReferenceField' in str(field):
                res.append({ 'fields': [(name, 1)] })
            if 'fields.CachedReferenceField' in str(field):
                res.append({ 'fields': [("%s._id" % name, 1)] })
        return res
        
    @classmethod
    def _build_history_indices(cls):
        if not cls.__name__.startswith("Historical"):
            return []
        indexes = []
        autoIdIndex = {'fields': [('_auto_id_0', 1)], 'args': {'noCompanyPrefix': True}}
        indexes.append(autoIdIndex)

        historyTTL = cls.instance_type._meta.get('historyTTL')
        if historyTTL:
            delete_only_eta_created_entries = False
            if isinstance(historyTTL, int):
                ttl = historyTTL
            else:
                ttl = historyTTL.get('ttl')
                delete_only_eta_created_entries = historyTTL.get('deleteOnlyEtaCreatedEntries', False)

            historyTTLIndex = {'fields': [('history_date', -1)], 'expireAfterSeconds': ttl,
                               'args': {'noCompanyPrefix': True}}
            if delete_only_eta_created_entries:
                # partial filter expressions only support True conditions
                historyTTLIndex['partialFilterExpression'] = {
                    'eta_id': {'$exists': True}
                }

            indexes.append(historyTTLIndex)
        else:
            # append an index on history_date,
            # _rippling_process_index_spec will take care of adding company prefix if needed
            indexes.append({'fields': [('history_date', -1)]})
            indexes.append({'fields': [('history_date', -1), ('_auto_id_0', 1)]})
            indexes.append({'fields': [('_auto_id_0', 1), ('history_date', -1), ('_id', -1)]})

        return indexes
        
    @classmethod
    def _rippling_process_index_spec(cls, spec):
        # Remove `company` for spec['fields']
        spec['fields'] = [field for field in spec['fields'] if field[0] != 'company']

        if "expireAfterSeconds" in spec:
            args = spec.get("args", {})
            args['noCompanyPrefix'] = True
            spec['args'] = args
        
        # Add `company` forcibly to the front.
        noCompanyPrefix = spec.pop('args', {}).get('noCompanyPrefix', False)
        if not noCompanyPrefix and cls._fields.get('company'):
            spec['fields'].insert(0, ('company', 1))
            
        return spec

    @classmethod
    def _build_index_spec(cls, spec):
        """Build a PyMongo index spec from a MongoEngine index spec.
        """
        if isinstance(spec, string_types):
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
                    if isinstance(field.unique_with, string_types):
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
