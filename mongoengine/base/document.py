import copy
import numbers
from functools import partial

from bson import DBRef, ObjectId, SON, json_util
import pymongo
import six
from six import iteritems

from mongoengine import signals
from mongoengine.base.common import get_document
from mongoengine.base.datastructures import (
    BaseDict,
    BaseList,
    EmbeddedDocumentList,
    LazyReference,
    StrictDict,
)

from mongoengine.base.fields import ComplexBaseField
from mongoengine.common import _import_class, ReadOnlyContext
from mongoengine.errors import (
    FieldDoesNotExist,
    InvalidDocumentError,
    LookUpError,
    OperationError,
    ValidationError,
)
from mongoengine.python_support import Hashable
from mongoengine.base.proxy import DocumentProxy
from mongoengine.connection import DEFAULT_CONNECTION_NAME

__all__ = ("BaseDocument", "NON_FIELD_ERRORS")

NON_FIELD_ERRORS = "__all__"


class BaseDocument(object):
    # TODO simplify how `_changed_fields` is used.
    # Currently, handling of `_changed_fields` seems unnecessarily convoluted:
    # 1. `BaseDocument` defines `_changed_fields` in its `__slots__`, yet it's
    #    not setting it to `[]` (or any other value) in `__init__`.
    # 2. `EmbeddedDocument` sets `_changed_fields` to `[]` it its overloaded
    #    `__init__`.
    # 3. `Document` does NOT set `_changed_fields` upon initialization. The
    #    field is primarily set via `_from_son` or `_clear_changed_fields`,
    #    though there are also other methods that manipulate it.
    # 4. The codebase is littered with `hasattr` calls for `_changed_fields`.

    _dynamic = False
    _dynamic_lock = True
    STRICT = False

    def __init__(self, *args, **values):
        """
        Initialise a document or an embedded document.

        :param values: A dictionary of keys and values for the document.
            It may contain additional reserved keywords, e.g. "__auto_convert".
        :param __auto_convert: If True, supplied values will be converted
            to Python-type values via each field's `to_python` method.
        :param __only_fields: A set of fields that have been loaded for
            this document. Empty if all fields have been loaded.
        :param _created: Indicates whether this is a brand new document
            or whether it's already been persisted before. Defaults to true.
        """
        super(BaseDocument, self).__init__()
        self._initialised = False
        self._created = True

        kwargs_passed = values.pop("kwargs_passed", None)
        loading_from_db = values.pop("loading_from_db", False)
        if kwargs_passed is not None:
            values.update(kwargs_passed)

        if args:
            raise TypeError(
                "Instantiating a document with positional arguments is not "
                "supported. Please use `field_name=value` keyword arguments."
            )

        __auto_convert = values.pop("__auto_convert", True)

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
        for key, value in values.iteritems():
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
        for key, field in self._fields.iteritems():
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

        # Set any get_<field>_display methods
        self.__set_field_display()

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

            if not hasattr(self, name) and not name.startswith("_"):
                DynamicField = _import_class("DynamicField")
                field = DynamicField(db_field=name, null=True)
                field.name = name
                self._dynamic_fields[name] = field
                self._fields_ordered += (name,)

            if not name.startswith("_"):
                value = self.__expand_dynamic_values(name, value)

            # Handle marking data as changed
            if name in self._dynamic_fields:
                self._data[name] = value
                if hasattr(self, "_changed_fields"):
                    self._mark_as_changed(name)
        try:
            self__created = self._created
        except AttributeError:
            self__created = True

        if (
            self._is_document
            and not self__created
            and name in self._meta.get("shard_key", tuple())
            and self._data.get(name) != value
        ):
            msg = "Shard Keys are immutable. Tried to update %s" % name
            raise OperationError(msg)

        try:
            self__initialised = self._initialised
        except AttributeError:
            self__initialised = False

        # Check if the user has created a new instance of a class
        if (
            self._is_document
            and self__initialised
            and self__created
            and name == self._meta.get("id_field")
        ):
            super(BaseDocument, self).__setattr__("_created", False)

        super(BaseDocument, self).__setattr__(name, value)

    def setattr_quick(self, name, value):
        super(BaseDocument, self).__setattr__(name, value)

    def __getstate__(self):
        data = {}
        for k in (
            "_changed_fields",
            "_initialised",
            "_created",
            "_dynamic_fields",
            "_fields_ordered",
            "_only_fields",
        ):
            if hasattr(self, k):
                data[k] = getattr(self, k)
        data["_data"] = self.to_mongo()
        return data

    def __setstate__(self, data):
        if isinstance(data["_data"], SON):
            data["_data"] = self.__class__._from_son(data["_data"])._data
        for k in (
            "_changed_fields",
            "_initialised",
            "_created",
            "_data",
            "_dynamic_fields",
            "_only_fields",
        ):
            if k in data:
                setattr(self, k, data[k])
        if "_fields_ordered" in data:
            if self._dynamic:
                setattr(self, "_fields_ordered", data["_fields_ordered"])
            else:
                _super_fields_ordered = type(self)._fields_ordered
                setattr(self, "_fields_ordered", _super_fields_ordered)

        dynamic_fields = data.get("_dynamic_fields") or SON()
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
            u = "[Bad Unicode data]"
        repr_type = str if u is None else type(u)
        return repr_type("<%s: %s>" % (self.__class__.__name__, u))

    def __str__(self):
        # TODO this could be simpler?
        if hasattr(self, "__unicode__"):
            if six.PY3:
                return self.__unicode__()
            else:
                return six.text_type(self).encode("utf-8")
        return six.text_type("%s object" % self.__class__.__name__)

    def __eq__(self, other):
        if type(other) is DocumentProxy:
            return self.id == other.id
        if (
            isinstance(other, self.__class__)
            and hasattr(other, "id")
            and other.id is not None
        ):
            return self.id == other.id
        if isinstance(other, DBRef):
            return (
                self._get_collection_name() == other.collection and self.id == other.id
            )
        if self.id is None:
            return self is other
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

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

        if "_text_score" not in self._data:
            raise InvalidDocumentError(
                "This document is not originally built from a text query"
            )

        return self._data["_text_score"]

    def to_mongo(self, use_db_field=True, fields=None):
        """
        Return as SON data ready for use with MongoDB.
        """
        fields = fields or []

        data = SON()
        data["_id"] = None
        data["_cls"] = self._class_name

        # only root fields ['test1.a', 'test2'] => ['test1', 'test2']
        root_fields = {f.split(".")[0] for f in fields}

        for field_name in self:
            if root_fields and field_name not in root_fields:
                continue

            field = self._fields.get(field_name)

            if field and field.is_v2_field():
                value = self.v2_get(field)
            else:
                value = self._data.get(field_name, None)

            if value is not None:
                f_inputs = field.to_mongo.__code__.co_varnames
                ex_vars = {}
                if fields and "fields" in f_inputs:
                    key = "%s." % field_name
                    embedded_fields = [
                        i.replace(key, "") for i in fields if i.startswith(key)
                    ]

                    ex_vars["fields"] = embedded_fields

                if "use_db_field" in f_inputs:
                    ex_vars["use_db_field"] = use_db_field

                value = field.to_mongo(value, **ex_vars)

            # Handle self generating fields
            if value is None and field._auto_gen:
                value = field.generate()
                self._data[field_name] = value

            if (value is not None) or (field.null):
                if use_db_field:
                    data[field.db_field] = value
                else:
                    data[field.name] = value

        # Only add _cls if allow_inheritance is True
        if not self._meta.get("allow_inheritance"):
            data.pop("_cls")

        return data

    def validate(self, clean=True):
        """Ensure that all fields' values are valid and that required fields
        are present.

        Raises :class:`ValidationError` if any of the fields' values are found
        to be invalid.
        """
        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError as error:
                errors[NON_FIELD_ERRORS] = error

        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        GenericEmbeddedDocumentField = _import_class("GenericEmbeddedDocumentField")
        ComplexBaseField = _import_class("ComplexBaseField")

        for field_name in self._fields_ordered:
            field = self._fields.get(field_name, None)
            if field and field.is_v2_field():
                value = self.v2_get(field)
            else:
                value = self._data.get(field_name)
            if value is not None:
                try:
                    if isinstance(
                        field, (EmbeddedDocumentField, GenericEmbeddedDocumentField, ComplexBaseField)
                    ):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError) as error:
                    errors[field.name] = error
            elif field.required and not getattr(field, "_auto_gen", False):
                errors[field.name] = ValidationError(
                    "Field is required", field_name=field.name
                )

        if errors:
            pk = "None"
            if hasattr(self, "pk"):
                pk = self.pk
            elif self._instance and hasattr(self._instance, "pk"):
                pk = self._instance.pk
            message = "ValidationError (%s:%s) " % (self._class_name, pk)
            raise ValidationError(message, errors=errors)

    def to_json(self, *args, **kwargs):
        """Convert this document to JSON.

        :param use_db_field: Serialize field names as they appear in
            MongoDB (as opposed to attribute names on this document).
            Defaults to True.
        """
        use_db_field = kwargs.pop("use_db_field", True)
        return json_util.dumps(self.to_mongo(use_db_field), *args, **kwargs)

    @classmethod
    def from_json(cls, json_data, created=False):
        """Converts json data to a Document instance

        :param str json_data: The json data to load into the Document
        :param bool created: Boolean defining whether to consider the newly
            instantiated document as brand new or as persisted already:
            * If True, consider the document as brand new, no matter what data
              it's loaded with (i.e. even if an ID is loaded).
            * If False and an ID is NOT provided, consider the document as
              brand new.
            * If False and an ID is provided, assume that the object has
              already been persisted (this has an impact on the subsequent
              call to .save()).
            * Defaults to ``False``.
        """
        # TODO should `created` default to False? If the object already exists
        # in the DB, you would likely retrieve it from MongoDB itself through
        # a query, not load it from JSON data.
        return cls._from_son(json_util.loads(json_data), created=created)

    def __expand_dynamic_values(self, name, value):
        """Expand any dynamic values to their correct types / values."""
        if not isinstance(value, (dict, list, tuple)):
            return value

        # If the value is a dict with '_cls' in it, turn it into a document
        is_dict = isinstance(value, dict)
        if is_dict and "_cls" in value:
            cls = get_document(value["_cls"])
            return cls(**value)

        if is_dict:
            value = {k: self.__expand_dynamic_values(k, v) for k, v in value.items()}
        else:
            value = [self.__expand_dynamic_values(name, v) for v in value]

        # Convert lists / values so we can watch for any changes on them
        EmbeddedDocumentListField = _import_class("EmbeddedDocumentListField")
        if isinstance(value, (list, tuple)) and not isinstance(value, BaseList):
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
        """Mark a key as explicitly changed by the user."""
        if not key:
            return

        if not hasattr(self, "_changed_fields"):
            return

        if not hasattr(self, '_original_values'):
            self._original_values = {}

        model_key = key
        if "." in key:
            key, rest = key.split(".", 1)
            key = self._db_field_map.get(key, key)
            key = "%s.%s" % (key, rest)
        else:
            key = self._db_field_map.get(key, key)

        if key not in self._changed_fields:
            levels, idx = key.split("."), 1
            while idx <= len(levels):
                if ".".join(levels[:idx]) in self._changed_fields:
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
                level = ".".join(levels[:idx]) + "."
                remove = self._changed_fields.remove
                for field in self._changed_fields[:]:
                    if field.startswith(level):
                        remove(field)

    def _clear_changed_fields(self):
        """Using _get_changed_fields iterate and remove any fields that
        are marked as changed.
        """
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

                if not isinstance(data, LazyReference) and hasattr(
                    data, "_changed_fields"
                ):
                    if getattr(data, "_is_document", False):
                        continue

                    data._changed_fields = []

        self._changed_fields = []
        self._original_values = {}
        self._force_changed_fields = set()

    def _nestable_types_changed_fields(self, changed_fields, base_key, data):
        """Inspect nested data for changed fields

        :param changed_fields: Previously collected changed fields
        :param base_key: The base key that must be used to prepend changes to this data
        :param data: data to inspect for changes
        """
        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, "items"):
            iterator = enumerate(data)
        else:
            iterator = iteritems(data)

        for index_or_key, value in iterator:
            item_key = "%s%s." % (base_key, index_or_key)
            # don't check anything lower if this key is already marked
            # as changed.
            if item_key[:-1] in changed_fields:
                continue

            if hasattr(value, "_get_changed_fields"):
                changed = value._get_changed_fields()
                changed_fields += ["%s%s" % (item_key, k) for k in changed if k]
            elif isinstance(value, (list, tuple, dict)):
                self._nestable_types_changed_fields(changed_fields, item_key, value)

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
        """Return a list of all fields that have explicitly been changed.
        """
        EmbeddedDocument = _import_class("EmbeddedDocument")
        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")
        SortedListField = _import_class("SortedListField")

        changed_fields = getattr(self, '_changed_fields', [])
        original_values = getattr(self, '_original_values', {})

        for field_name in self._fields_ordered:
            db_field_name = self._db_field_map.get(field_name, field_name)
            key = "%s." % db_field_name
            data = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if db_field_name in changed_fields:
                # Whole field already marked as changed, no need to go further
                continue

            if isinstance(field, ReferenceField):  # Don't follow referenced documents
                continue

            if isinstance(data, EmbeddedDocument):
                # Find all embedded fields that have been changed
                changed = data._get_changed_fields()
                for k in changed:
                    if k:
                        field_name = "%s%s" % (key, k)
                        if field_name not in changed_fields:
                            changed_fields.append(field_name)
                            if k in getattr(data, '_original_values', {}):
                                original_values[field_name] = getattr(data, '_original_values', {})[k]
            elif isinstance(data, (list, tuple, dict)):
                if hasattr(field, "field") and isinstance(
                    field.field, (ReferenceField, GenericReferenceField)
                ):
                    continue
                elif isinstance(field, SortedListField) and field._ordering:
                    # if ordering is affected whole list is changed
                    if any(field._ordering in d._changed_fields for d in data):
                        changed_fields.append(db_field_name)
                        continue

                self._nestable_types_changed_fields(changed_fields, key, data)
        return changed_fields

    def _delta(self):
        """Returns the delta (set, unset) of the changes for a document.
        Gets any values that have been explicitly changed.
        """
        # Handles cases where not loaded from_son but has _id
        doc = self.to_mongo()

        set_fields = self._get_changed_fields()
        unset_data = {}
        if hasattr(self, "_changed_fields"):
            set_data = {}
            # Fetch each set item from its path
            for path in set_fields:
                parts = path.split(".")
                d = doc
                new_path = []
                for p in parts:
                    if isinstance(d, (ObjectId, DBRef)):
                        # Don't dig in the references
                        break
                    elif isinstance(d, list) and p.isdigit():
                        # An item of a list (identified by its index) is updated
                        d = d[int(p)]
                    elif hasattr(d, "get"):
                        # dict-like (dict, embedded document)
                        d = d.get(p)
                    new_path.append(p)
                path = ".".join(new_path)
                set_data[path] = d
        else:
            set_data = doc
            if "_id" in set_data:
                del set_data["_id"]

        # Determine if any changed items were actually unset.
        for path, value in set_data.items():
            if value or isinstance(
                value, (numbers.Number, bool, list)
            ):  # Account for 0 and True that are truthy
                continue

            parts = path.split(".")

            if self._dynamic and len(parts) and parts[0] in self._dynamic_fields:
                del set_data[path]
                unset_data[path] = 1
                continue

            # If we've set a value that ain't the default value don't unset it.
            default = None
            to_unset = True
            if path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                d_type = type(self)
                db_field_name = parts.pop()
                for p in parts:
                    if isinstance(d, list) and p.isdigit():
                        d_next = d[int(p)]
                    elif hasattr(d, "__getattribute__") and not isinstance(d, dict):
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

                if hasattr(d, "_fields"):
                    field_name = d._reverse_db_field_map.get(
                        db_field_name, db_field_name
                    )
                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None
                elif isinstance(d_type, DictField):
                    to_unset = False

            if default is not None:
                default = default() if callable(default) else default

            if not to_unset or default != value:
                continue

            del set_data[path]
            unset_data[path] = 1
        return set_data, unset_data

    @classmethod
    def _get_collection_name(cls):
        """Return the collection name for this class. None for abstract
        class.
        """
        return cls._meta.get("collection", None)

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
        """Create an instance of a Document (subclass) from a PyMongo SON."""
        if not only_fields:
            only_fields = []

        if son and not isinstance(son, dict):
            raise ValueError(
                "The source SON object needs to be of type 'dict' but a '%s' was found"
                % type(son)
            )

        # Get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get("_cls", cls._class_name)

        # Convert SON to a data dict, making sure each key is a string and
        # corresponds to the right db field.
        data = {}
        for key, value in iteritems(son):
            key = str(key)
            key = cls._db_field_map.get(key, key)
            data[key] = value

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        errors_dict = {}

        fields = cls._fields
        if not _auto_dereference:
            fields = copy.deepcopy(fields)

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
                            has_reference = isinstance(field,
                                                       (ReferenceField, CachedReferenceField, EmbeddedDocumentField))
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

        if errors_dict:
            errors = "\n".join(
                ["Field '%s' - %s" % (k, v) for k, v in errors_dict.items()]
            )
            msg = "Invalid data to create a `%s` instance.\n%s" % (
                cls._class_name,
                errors,
            )
            raise InvalidDocumentError(msg)

        # In STRICT documents, remove any keys that aren't in cls._fields
        if cls.STRICT:
            data = {k: v for k, v in iteritems(data) if k in cls._fields}

        obj = cls(__auto_convert=False, _created=created, __only_fields=only_fields, kwargs_passed=data,
                  loading_from_db=loading_from_db)
        obj._changed_fields = []
        if not _auto_dereference:
            obj._fields = fields

        return obj

    @classmethod
    def _build_index_specs(cls, meta_indexes):
        """Generate and merge the full index specs."""
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
        for name, field in cls._fields.iteritems():
            if 'fields.ReferenceField' in str(field):
                res.append({'fields': [(name, 1)]})
            if 'fields.CachedReferenceField' in str(field):
                res.append({'fields': [("%s._id" % name, 1)]})
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
        # Add `company` forcibly to the front.
        noCompanyPrefix = spec.pop('args', {}).get('noCompanyPrefix', False)
        if not noCompanyPrefix and cls._fields.get('company'):
            spec['fields'].insert(0, ('company', 1))
        return spec

    @classmethod
    def _build_index_spec(cls, spec):
        """Build a PyMongo index spec from a MongoEngine index spec."""
        if isinstance(spec, six.string_types):
            spec = {"fields": [spec]}
        elif isinstance(spec, (list, tuple)):
            spec = {"fields": list(spec)}
        elif isinstance(spec, dict):
            spec = dict(spec)

        index_list = []
        direction = None

        # Check to see if we need to include _cls
        allow_inheritance = cls._meta.get("allow_inheritance")
        include_cls = (
            allow_inheritance
            and not spec.get("sparse", False)
            and spec.get("cls", True)
            and "_cls" not in spec["fields"]
        )

        # 733: don't include cls if index_cls is False unless there is an explicit cls with the index
        include_cls = include_cls and (
            spec.get("cls", False) or cls._meta.get("index_cls", True)
        )
        if "cls" in spec:
            spec.pop("cls")
        for key in spec["fields"]:
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
            parts = key.split(".")
            if parts in (["pk"], ["id"], ["_id"]):
                key = "_id"
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
                key = ".".join(parts)
            index_list.append((key, direction))

        # Don't add cls to a geo index
        if include_cls and direction not in (
            pymongo.GEO2D,
            pymongo.GEOHAYSTACK,
            pymongo.GEOSPHERE,
        ):
            index_list.insert(0, ("_cls", 1))

        if index_list:
            spec["fields"] = index_list

        return spec

    @classmethod
    def _unique_with_indexes(cls, namespace=""):
        """Find unique indexes in the document schema and return them."""
        unique_indexes = []
        for field_name, field in cls._fields.items():
            sparse = field.sparse

            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, six.string_types):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with = []
                    for other_name in field.unique_with:
                        parts = other_name.split(".")

                        # Lookup real name
                        parts = cls._lookup_field(parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append(".".join(name_parts))

                        # Unique field should be required
                        parts[-1].required = True
                        sparse = not sparse and parts[-1].name not in cls.__dict__

                    unique_fields += unique_with

                # Add the new index to the list
                fields = [
                    ("%s%s" % (namespace, f), pymongo.ASCENDING) for f in unique_fields
                ]
                index = {"fields": fields, "unique": True, "sparse": sparse}
                unique_indexes.append(index)

            if field.__class__.__name__ in {
                "EmbeddedDocumentListField",
                "ListField",
                "SortedListField",
            }:
                field = field.field

            # Grab any embedded document field unique indexes
            if (
                field.__class__.__name__ == "EmbeddedDocumentField"
                and field.document_type != cls
            ):
                field_namespace = "%s." % field_name
                doc_cls = field.document_type
                unique_indexes += doc_cls._unique_with_indexes(field_namespace)

        return unique_indexes

    @classmethod
    def _geo_indices(cls, inspected=None, parent_field=None):
        inspected = inspected or []
        geo_indices = []
        inspected.append(cls)

        geo_field_type_names = (
            "EmbeddedDocumentField",
            "GeoPointField",
            "PointField",
            "LineStringField",
            "PolygonField",
        )

        geo_field_types = tuple(
            [_import_class(field) for field in geo_field_type_names]
        )

        for field in cls._fields.values():
            if not isinstance(field, geo_field_types):
                continue

            if hasattr(field, "document_type"):
                field_cls = field.document_type
                if field_cls in inspected:
                    continue

                if hasattr(field_cls, "_geo_indices"):
                    geo_indices += field_cls._geo_indices(
                        inspected, parent_field=field.db_field
                    )
            elif field._geo_index:
                field_name = field.db_field
                if parent_field:
                    field_name = "%s.%s" % (parent_field, field_name)
                geo_indices.append({"fields": [(field_name, field._geo_index)]})

        return geo_indices

    @classmethod
    def _lookup_field(cls, parts):
        """Given the path to a given field, return a list containing
        the Field object associated with that field and all of its parent
        Field objects.

        Args:
            parts (str, list, or tuple) - path to the field. Should be a
            string for simple fields existing on this document or a list
            of strings for a field that exists deeper in embedded documents.

        Returns:
            A list of Field instances for fields that were found or
            strings for sub-fields that weren't.

        Example:
            >>> user._lookup_field('name')
            [<mongoengine.fields.StringField at 0x1119bff50>]

            >>> user._lookup_field('roles')
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>]

            >>> user._lookup_field(['roles', 'role'])
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>,
             <mongoengine.fields.StringField at 0x1119ec050>]

            >>> user._lookup_field('doesnt_exist')
            raises LookUpError

            >>> user._lookup_field(['roles', 'doesnt_exist'])
            [<mongoengine.fields.EmbeddedDocumentListField at 0x1119ec250>,
             'doesnt_exist']

        """
        # TODO this method is WAY too complicated. Simplify it.
        # TODO don't think returning a string for embedded non-existent fields is desired

        ListField = _import_class("ListField")
        DynamicField = _import_class("DynamicField")

        if not isinstance(parts, (list, tuple)):
            parts = [parts]

        fields = []
        field = None

        for field_name in parts:
            # Handle ListField indexing:
            if field_name.isdigit() and isinstance(field, ListField):
                fields.append(field_name)
                continue

            # Look up first field from the document
            if field is None:
                if field_name == "pk":
                    # Deal with "primary key" alias
                    field_name = cls._meta["id_field"]

                if field_name in cls._fields:
                    field = cls._fields[field_name]
                elif cls._dynamic:
                    field = DynamicField(db_field=field_name)
                elif cls._meta.get("allow_inheritance") or cls._meta.get(
                    "abstract", False
                ):
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
                    raise LookUpError('Cannot resolve field "%s"' % field_name)
            else:
                ReferenceField = _import_class("ReferenceField")
                GenericReferenceField = _import_class("GenericReferenceField")

                # If previous field was a reference, throw an error (we
                # cannot look up fields that are on references).
                if isinstance(field, (ReferenceField, GenericReferenceField)):
                    raise LookUpError(
                        "Cannot perform join in mongoDB: %s" % "__".join(parts)
                    )

                # If the parent field has a "field" attribute which has a
                # lookup_member method, call it to find the field
                # corresponding to this iteration.
                if hasattr(getattr(field, "field", None), "lookup_member"):
                    new_field = field.field.lookup_member(field_name)

                # If the parent field is a DynamicField or if it's part of
                # a DynamicDocument, mark current field as a DynamicField
                # with db_name equal to the field name.
                elif cls._dynamic and (
                    isinstance(field, DynamicField)
                    or getattr(getattr(field, "document_type", None), "_dynamic", None)
                ):
                    new_field = DynamicField(db_field=field_name)

                # Else, try to use the parent field's lookup_member method
                # to find the subfield.
                elif hasattr(field, "lookup_member"):
                    new_field = field.lookup_member(field_name)

                # Raise a LookUpError if all the other conditions failed.
                else:
                    raise LookUpError(
                        "Cannot resolve subfield or operator {} "
                        "on the field {}".format(field_name, field.name)
                    )

                # If current field still wasn't found and the parent field
                # is a ComplexBaseField, add the name current field name and
                # move on.
                if not new_field and isinstance(field, ComplexBaseField):
                    fields.append(field_name)
                    continue
                elif not new_field:
                    raise LookUpError('Cannot resolve field "%s"' % field_name)

                field = new_field  # update field to the new field type

            fields.append(field)

        return fields

    @classmethod
    def _translate_field_name(cls, field, sep="."):
        """Translate a field attribute name to a database field name.
        """
        parts = field.split(sep)
        parts = [f.db_field for f in cls._lookup_field(parts)]
        return ".".join(parts)

    def __set_field_display(self):
        """For each field that specifies choices, create a
        get_<field>_display method.
        """
        fields_with_choices = [(n, f) for n, f in self._fields.items() if f.choices]
        for attr_name, field in fields_with_choices:
            setattr(
                self,
                "get_%s_display" % attr_name,
                partial(self.__get_field_display, field=field),
            )

    def __get_field_display(self, field):
        """Return the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            if value is None:
                return None
            sep = getattr(field, "display_sep", " ")
            values = (
                value
                if field.__class__.__name__ in ("ListField", "SortedListField")
                else [value]
            )
            return sep.join(
                [
                    six.text_type(dict(field.choices).get(val, val))
                    for val in values or []
                ]
            )
        return value
