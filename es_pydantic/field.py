import ipaddress
from datetime import datetime, date
from typing import Optional

from dateutil import parser


class _FieldES:
    __slots__ = {"_elastic_fields"}

    # ES type name
    type = None

    def __init__(self, **elasticsearch_kwargs):
        self._elastic_fields = elasticsearch_kwargs

    def __getattr__(self, item):
        return getattr(self._elastic_fields, item)

    def __str__(self):
        return f"{self.__class__.__name__}: {self.type}"

    def __repr__(self):
        return f"<Field: {self.__class__.__name__}: {self.type}>"

    def to_dict(self):
        # type, etc.
        cls_items = self.__class__.__dict__.items()
        # avoid internals
        cls_dict = {
            key: val
            for key, val in cls_items
            if not key.startswith("__") and isinstance(val, str)
        }
        # combine class vars and self defined vars
        d = {**cls_dict, **self._elastic_fields}
        d = {key: val for key, val in d.items() if val}
        return d

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, data):
        # no validation
        return data


class Version(_FieldES):
    type = "version"

    @classmethod
    def validate(cls, data):
        if not isinstance(data, str):
            raise ValueError("'version' should be string")
        return data


class Text(_FieldES):
    type = "text"


class Integer(_FieldES):
    type = "integer"

    @classmethod
    def validate(cls, data):
        try:
            return int(data)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid integer value {data}") from None


class Float(_FieldES):
    type = "float"

    @classmethod
    def validate(cls, data):
        try:
            return float(data)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid float value {data}") from None


class Keyword(_FieldES):
    type = "keyword"

    def __init__(self, ignore_above: Optional[int] = None):
        super().__init__(ignore_above=ignore_above)

    @classmethod
    def validate(cls, data):
        if not data:
            raise ValueError("'Keyword' field should not be empty")
        return data


class Boolean(_FieldES):
    type = "boolean"

    @classmethod
    def validate(cls, data):
        if data in [False, "false"]:
            return False
        return bool(data)


class IP(_FieldES):
    type = "ip"

    @classmethod
    def validate(cls, data):
        ipaddress.ip_address(data)
        # ipaddress.AddressValueError is a ValueError exception
        return str(data)


class DateTime(_FieldES):
    type = "date"

    @classmethod
    def validate(cls, data):
        if isinstance(data, (date, datetime)):
            return data
        if isinstance(data, str):
            try:
                return parser.parse(data)
            except Exception as e:
                raise ValueError(f"Could not parse date from the value ({data}") from e
        if isinstance(data, int):
            # Divide by a float to preserve milliseconds on the datetime.
            return datetime.utcfromtimestamp(data / 1000.0)

        raise ValueError(f"Could not parse date from value {data}")
