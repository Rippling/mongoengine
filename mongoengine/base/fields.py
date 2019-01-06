import operator
import warnings
import weakref

from bson import DBRef, ObjectId, SON
import pymongo
from mongoengine.base.proxy import DocumentProxy

from mongoengine.common import _import_class
from mongoengine.errors import ValidationError
from mongoengine.base.common import ALLOW_INHERITANCE
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList
)

__all__ = ("BaseField", "ComplexBaseField",
           "ObjectIdField", "GeoJsonBaseField")


UPDATE_OPERATORS = set(['set', 'unset', 'inc', 'dec', 'pop', 'push',
                        'push_all', 'pull', 'pull_all', 'add_to_set',
                        'set_on_insert', 'min', 'max'])

V2_OPTIMIZED_FIELDS = set(["DecimalField", "DateTimeField"])

class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.

    .. versionchanged:: 0.5 - added verbose and help text
    """

    name = None
    _geo_index = False
    _auto_gen = False  # Call `generate` to generate a value
    _auto_dereference = True

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that MongoEngine implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1

    def __init__(self, db_field=None, name=None, required=False, default=None,
                 unique=False, unique_with=None, primary_key=False,
                 validation=None, choices=None, null=False, sparse=False,
                 **kwargs):
        """
        :param db_field: The database field to store this field in
            (defaults to the name of the field)
        :param name: Depreciated - use db_field
        :param required: If the field is required. Whether it has to have a
            value or not. Defaults to False.
        :param default: (optional) The default value for this field if no value
            has been set (or if the value has been unset).  It can be a
            callable.
        :param unique: Is the field value unique or not.  Defaults to False.
        :param unique_with: (optional) The other field this field should be
            unique with.
        :param primary_key: Mark this field as the primary key. Defaults to False.
        :param validation: (optional) A callable to validate the value of the
            field.  Generally this is deprecated in favour of the
            `FIELD.validate` method
        :param choices: (optional) The valid choices
        :param null: (optional) Is the field value can be null. If no and there is a default value
            then the default value is set
        :param sparse: (optional) `sparse=True` combined with `unique=True` and `required=False`
            means that uniqueness won't be enforced for `None` values
        :param **kwargs: (optional) Arbitrary indirection-free metadata for
            this field can be supplied as additional keyword arguments and
            accessed as attributes of the field. Must not conflict with any
            existing attributes. Common metadata includes `verbose_name` and
            `help_text`.
        """
        self.db_field = (db_field or name) if not primary_key else '_id'

        if name:
            msg = "Fields' 'name' attribute deprecated in favour of 'db_field'"
            warnings.warn(msg, DeprecationWarning)
        self.required = required or primary_key
        self.default = default
        self.unique = bool(unique or unique_with)
        self.unique_with = unique_with
        self.primary_key = primary_key
        self.validation = validation
        self.choices = choices
        self.null = null
        self.sparse = sparse
        self._owner_document = None
        
        # Detect and report conflicts between metadata and base properties.
        conflicts = set(dir(self)) & set(kwargs)
        if conflicts:
            raise TypeError("%s already has attribute(s): %s" % (
                self.__class__.__name__, ', '.join(conflicts) ))
        
        # Assign metadata to the instance
        # This efficient method is available because no __slots__ are defined.
        self.__dict__.update(kwargs)

        # Adjust the appropriate creation counter, and save our local copy.
        if self.db_field == '_id':
            self.creation_counter = BaseField.auto_creation_counter
            BaseField.auto_creation_counter -= 1
        else:
            self.creation_counter = BaseField.creation_counter
            BaseField.creation_counter += 1

    def __get__(self, instance, owner):
        """Descriptor for retrieving a value from a field in a document.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        if self.is_v2_field():
            return self.v2_get(instance)
            
        # Get value from document instance if available
        return instance._data.get(self.name)

    def v2_get(self, instance):
        return instance.v2_get(self)
        
        
    def is_v2_field(self):
        return self.__class__.__name__ in V2_OPTIMIZED_FIELDS

    def _should_mark_as_changed(self, instance, value):
        if self.name not in instance._data:
            # New value should not be None
            return value is not None
        return instance._data[self.name] != value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document.
        """

        # If setting to None and there is a default
        # Then set the value to the default value
        if value is None:
            if self.null:
                value = None
            elif self.default is not None:
                value = self.default
                if callable(value):
                    value = value()
            elif not self.required and self.name not in instance._data:
                # If the field is optional and the value is None, we don't need to do anything
                return

        if instance._initialised:
            try:
                if self._should_mark_as_changed(instance, value):
                    instance._mark_as_changed(self.name)
            except Exception:
                # Values cant be compared eg: naive and tz datetimes
                # So mark it as changed
                instance._mark_as_changed(self.name)

        if not type(value) is DocumentProxy:
            EmbeddedDocument = _import_class('EmbeddedDocument')
            if isinstance(value, EmbeddedDocument):
                value._instance = weakref.proxy(instance)
            elif isinstance(value, (list, tuple)):
                for v in value:
                    if type(v) is not DocumentProxy and isinstance(v, EmbeddedDocument):
                        v._instance = weakref.proxy(instance)
        else:
            # Set this for DocumentProxy as well
            value._instance = weakref.proxy(instance)
        
        if self.is_v2_field():
            instance.v2_set(self, value)
        else:
            instance._data[self.name] = value

    def error(self, message="", errors=None, field_name=None):
        """Raises a ValidationError.
        """
        field_name = field_name if field_name else self.name
        raise ValidationError(message, errors=errors, field_name=field_name)

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        return value

    def to_mongo(self, value, **kwargs):
        """Convert a Python type to a MongoDB-compatible type.
        """
        return self.to_python(value)

    def prepare_query_value(self, op, value):
        """Prepare a value that is being used in a query for PyMongo.
        """
        if op in UPDATE_OPERATORS:
            self.validate(value)
        return value

    def validate(self, value, clean=True):
        """Perform validation on a value.
        """
        pass

    def _validate_choices(self, value):
        Document = _import_class('Document')
        EmbeddedDocument = _import_class('EmbeddedDocument')

        choice_list = self.choices
        if isinstance(choice_list[0], (list, tuple)):
            choice_list = [k for k, _ in choice_list]

        # Choices which are other types of Documents
        if isinstance(value, (Document, EmbeddedDocument)):
            if not any(isinstance(value, c) for c in choice_list):
                self.error(
                    'Value must be instance of %s' % unicode(choice_list)
                )
        # Choices which are types other than Documents
        elif value not in choice_list:
            self.error('Value must be one of %s' % unicode(choice_list))


    def _validate(self, value, **kwargs):
        # Check the Choices Constraint
        if self.choices:
            self._validate_choices(value)

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    self.error('Value does not match custom validation method')
            else:
                raise ValueError('validation argument for "%s" must be a '
                                 'callable.' % self.name)

        self.validate(value, **kwargs)

    @property
    def owner_document(self):
        return self._owner_document

    def _set_owner_document(self, owner_document):
        self._owner_document = owner_document

    @owner_document.setter
    def owner_document(self, owner_document):
        self._set_owner_document(owner_document)


class ComplexBaseField(BaseField):
    """Handles complex fields, such as lists / dictionaries.

    Allows for nesting of embedded documents inside complex types.
    Handles the lazy dereferencing of a queryset by lazily dereferencing all
    items in a list / dict rather than one at a time.

    .. versionadded:: 0.5
    """

    field = None

    def __get__(self, instance, owner):
        """Descriptor to automatically dereference references.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        EmbeddedDocumentListField = _import_class('EmbeddedDocumentListField')

        value = super(ComplexBaseField, self).__get__(instance, owner)

        # Convert lists / values so we can watch for any changes on them
        if isinstance(value, (list, tuple)):
            if (issubclass(type(self), EmbeddedDocumentListField) and
                    not isinstance(value, EmbeddedDocumentList)):
                value = EmbeddedDocumentList(value, instance, self.name)
            elif not isinstance(value, BaseList):
                value = BaseList(value, instance, self.name)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value

        return value

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        if isinstance(value, basestring):
            return value

        if hasattr(value, 'to_python'):
            return value.to_python()

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = dict([(k, v) for k, v in enumerate(value)])
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            self.field._auto_dereference = self._auto_dereference
            value_dict = dict([(key, self.field.to_python(item))
                               for key, item in value.items()])
        else:
            Document = _import_class('Document')
            value_dict = {}
            for k, v in value.items():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error('You can only reference documents once they'
                                   ' have been saved to the database')
                    collection = v._get_collection_name()
                    value_dict[k] = DBRef(collection, v.pk)
                elif hasattr(v, 'to_python'):
                    value_dict[k] = v.to_python()
                else:
                    value_dict[k] = self.to_python(v)

        if is_list:  # Convert back to a list
            return [v for _, v in sorted(value_dict.items(),
                                         key=operator.itemgetter(0))]
        return value_dict

    def to_mongo(self, value, **kwargs):
        """Convert a Python type to a MongoDB-compatible type.
        """
        Document = _import_class("Document")
        EmbeddedDocument = _import_class("EmbeddedDocument")
        GenericReferenceField = _import_class("GenericReferenceField")

        if isinstance(value, basestring):
            return value

        if hasattr(value, 'to_mongo'):
            if isinstance(value, Document):
                return GenericReferenceField().to_mongo(
                    value, **kwargs)
            cls = value.__class__
            val = value.to_mongo(**kwargs)
            # If it's a document that is not inherited add _cls
            if isinstance(value, EmbeddedDocument):
                val['_cls'] = cls.__name__
            return val

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = dict([(k, v) for k, v in enumerate(value)])
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = dict([(key, self.field.to_mongo(item, **kwargs))
                               for key, item in value.iteritems()])
        else:
            value_dict = {}
            for k, v in value.iteritems():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error('You can only reference documents once they'
                                   ' have been saved to the database')

                    # If its a document that is not inheritable it won't have
                    # any _cls data so make it a generic reference allows
                    # us to dereference
                    meta = getattr(v, '_meta', {})
                    allow_inheritance = (
                        meta.get('allow_inheritance', ALLOW_INHERITANCE)
                        is True)
                    if not allow_inheritance and not self.field:
                        value_dict[k] = GenericReferenceField().to_mongo(
                            v, **kwargs)
                    else:
                        collection = v._get_collection_name()
                        value_dict[k] = DBRef(collection, v.pk)
                elif hasattr(v, 'to_mongo'):
                    cls = v.__class__
                    val = v.to_mongo(**kwargs)
                    # If it's a document that is not inherited add _cls
                    if isinstance(v, (Document, EmbeddedDocument)):
                        val['_cls'] = cls.__name__
                    value_dict[k] = val
                else:
                    value_dict[k] = self.to_mongo(v, **kwargs)

        if is_list:  # Convert back to a list
            return [v for _, v in sorted(value_dict.items(),
                                         key=operator.itemgetter(0))]
        return value_dict

    def validate(self, value, clean=True):
        """If field is provided ensure the value is valid.
        """
        errors = {}

        if self.field:
            if hasattr(value, 'iteritems') or hasattr(value, 'items'):
                sequence = value.iteritems()
            else:
                sequence = enumerate(value)

            EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
            GenericEmbeddedDocumentField = _import_class("GenericEmbeddedDocumentField")

            for k, v in sequence:
                try:
                    if isinstance(self.field, (EmbeddedDocumentField,
                                               GenericEmbeddedDocumentField)):
                        self.field._validate(v, clean=clean)
                    else:
                        self.field._validate(v)
                except ValidationError, error:
                    errors[k] = error.errors or error
                except (ValueError, AssertionError), error:
                    errors[k] = error

            if errors:
                field_class = self.field.__class__.__name__
                self.error('Invalid %s item (%s)' % (field_class, value),
                           errors=errors)
        # Don't allow empty values if required
        if self.required and not value:
            self.error('Field is required and cannot be empty')

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def lookup_member(self, member_name):
        if self.field:
            return self.field.lookup_member(member_name)
        return None

    def _set_owner_document(self, owner_document):
        if self.field:
            self.field.owner_document = owner_document
        self._owner_document = owner_document


class ObjectIdField(BaseField):
    """A field wrapper around MongoDB's ObjectIds.
    """

    def to_python(self, value):
        try:
            if not isinstance(value, ObjectId):
                value = ObjectId(value)
        except Exception:
            pass
        return value

    def to_mongo(self, value, **kwargs):
        if not isinstance(value, ObjectId):
            try:
                return ObjectId(unicode(value))
            except Exception, e:
                # e.message attribute has been deprecated since Python 2.6
                self.error(unicode(e))
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        try:
            ObjectId(unicode(value))
        except Exception:
            self.error('Invalid Object ID')


class GeoJsonBaseField(BaseField):
    """A geo json field storing a geojson style object.

    .. versionadded:: 0.8
    """

    _geo_index = pymongo.GEOSPHERE
    _type = "GeoBase"

    def __init__(self, auto_index=True, *args, **kwargs):
        """
        :param bool auto_index: Automatically create a "2dsphere" index.\
            Defaults to `True`.
        """
        self._name = "%sField" % self._type
        if not auto_index:
            self._geo_index = False
        super(GeoJsonBaseField, self).__init__(*args, **kwargs)

    def validate(self, value):
        """Validate the GeoJson object based on its type
        """
        if isinstance(value, dict):
            if set(value.keys()) == set(['type', 'coordinates']):
                if value['type'] != self._type:
                    self.error('%s type must be "%s"' %
                               (self._name, self._type))
                return self.validate(value['coordinates'])
            else:
                self.error('%s can only accept a valid GeoJson dictionary'
                           ' or lists of (x, y)' % self._name)
                return
        elif not isinstance(value, (list, tuple)):
            self.error('%s can only accept lists of [x, y]' % self._name)
            return

        validate = getattr(self, "_validate_%s" % self._type.lower())
        error = validate(value)
        if error:
            self.error(error)

    def _validate_polygon(self, value, top_level=True):
        if not isinstance(value, (list, tuple)):
            return 'Polygons must contain list of linestrings'

        # Quick and dirty validator
        try:
            value[0][0][0]
        except (TypeError, IndexError):
            return "Invalid Polygon must contain at least one valid linestring"

        errors = []
        for val in value:
            error = self._validate_linestring(val, False)
            if not error and val[0] != val[-1]:
                error = 'LineStrings must start and end at the same point'
            if error and error not in errors:
                errors.append(error)
        if errors:
            if top_level:
                return "Invalid Polygon:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_linestring(self, value, top_level=True):
        """Validates a linestring"""
        if not isinstance(value, (list, tuple)):
            return 'LineStrings must contain list of coordinate pairs'

        # Quick and dirty validator
        try:
            value[0][0]
        except (TypeError, IndexError):
            return "Invalid LineString must contain at least one valid point"

        errors = []
        for val in value:
            error = self._validate_point(val)
            if error and error not in errors:
                errors.append(error)
        if errors:
            if top_level:
                return "Invalid LineString:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_point(self, value):
        """Validate each set of coords"""
        if not isinstance(value, (list, tuple)):
            return 'Points must be a list of coordinate pairs'
        elif not len(value) == 2:
            return "Value (%s) must be a two-dimensional point" % repr(value)
        elif (not isinstance(value[0], (float, int)) or
              not isinstance(value[1], (float, int))):
            return "Both values (%s) in point must be float or int" % repr(value)

    def _validate_multipoint(self, value):
        if not isinstance(value, (list, tuple)):
            return 'MultiPoint must be a list of Point'

        # Quick and dirty validator
        try:
            value[0][0]
        except (TypeError, IndexError):
            return "Invalid MultiPoint must contain at least one valid point"

        errors = []
        for point in value:
            error = self._validate_point(point)
            if error and error not in errors:
                errors.append(error)

        if errors:
            return "%s" % ", ".join(errors)

    def _validate_multilinestring(self, value, top_level=True):
        if not isinstance(value, (list, tuple)):
            return 'MultiLineString must be a list of LineString'

        # Quick and dirty validator
        try:
            value[0][0][0]
        except (TypeError, IndexError):
            return "Invalid MultiLineString must contain at least one valid linestring"

        errors = []
        for linestring in value:
            error = self._validate_linestring(linestring, False)
            if error and error not in errors:
                errors.append(error)

        if errors:
            if top_level:
                return "Invalid MultiLineString:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_multipolygon(self, value):
        if not isinstance(value, (list, tuple)):
            return 'MultiPolygon must be a list of Polygon'

        # Quick and dirty validator
        try:
            value[0][0][0][0]
        except (TypeError, IndexError):
            return "Invalid MultiPolygon must contain at least one valid Polygon"

        errors = []
        for polygon in value:
            error = self._validate_polygon(polygon, False)
            if error and error not in errors:
                errors.append(error)

        if errors:
            return "Invalid MultiPolygon:\n%s" % ", ".join(errors)

    def to_mongo(self, value, **kwargs):
        if isinstance(value, dict):
            return value
        return SON([("type", self._type), ("coordinates", value)])
