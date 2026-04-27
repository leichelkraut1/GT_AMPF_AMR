class MappingRecordBase(object):
    """
    Lightweight base for internal typed record objects.

    Mapping-style compatibility was intentionally transitional. Production
    callers should use direct attributes and only serialize with `toDict()`
    at outward boundaries.
    """

    FIELDS = ()
    EXTRA_FIELDS = ()

    @classmethod
    def _fieldNames(cls):
        return tuple(cls.FIELDS or ()) + tuple(cls.EXTRA_FIELDS or ())

    def _fieldDict(self):
        data = {}
        for fieldName in self.__class__._fieldNames():
            data[fieldName] = getattr(self, fieldName)
        return data

    def toDict(self):
        return self._fieldDict()

    def cloneWith(self, **overrides):
        fromDict = getattr(self.__class__, "fromDict", None)
        if fromDict is None:
            raise TypeError("{} does not support cloneWith without fromDict".format(self.__class__.__name__))

        data = self.toDict()
        data.update(dict(overrides or {}))
        return fromDict(data)

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join([
                "{}={!r}".format(fieldName, getattr(self, fieldName))
                for fieldName in self.__class__._fieldNames()
            ]),
        )


class RawBackedRecordBase(MappingRecordBase):
    RAW_FIELD_ALIASES = {}

    def __init__(self, rawData=None):
        self._raw_data = dict(rawData or {})

    def toDict(self):
        data = dict(self._raw_data)
        data.update(self._fieldDict())
        return data

    def get(self, key, default=None):
        if key in self.__class__._fieldNames():
            return getattr(self, key)
        return self._raw_data.get(key, default)

    def keys(self):
        keys = set(self.__class__._fieldNames())
        keys.update(list(self._raw_data.keys()))
        return list(keys)

    def items(self):
        return list(self.toDict().items())

    def values(self):
        return list(self.toDict().values())

    def __getitem__(self, key):
        if key in self.__class__._fieldNames():
            return getattr(self, key)
        if key not in self._raw_data:
            raise KeyError(key)
        return self._raw_data[key]

    def __contains__(self, key):
        return key in self.__class__._fieldNames() or key in self._raw_data

    def cloneWith(self, **overrides):
        fromDict = getattr(self.__class__, "fromDict", None)
        if fromDict is None:
            raise TypeError("{} does not support cloneWith without fromDict".format(self.__class__.__name__))

        data = self.toDict()
        aliasesByField = dict(getattr(self.__class__, "RAW_FIELD_ALIASES", {}) or {})
        for fieldName, aliasKeys in list(aliasesByField.items()):
            if fieldName not in overrides:
                continue
            for aliasKey in list(aliasKeys or []):
                data.pop(aliasKey, None)

        data.update(dict(overrides or {}))
        return fromDict(data)


def coerceText(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def coerceUpperText(value, default=None):
    text = coerceText(value, "" if default is None else default)
    if text is None:
        return None
    text = str(text).strip()
    if not text:
        return default
    return text.upper()


def coerceInt(value, default=0):
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def coerceIntOrNone(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def coerceFloatOrNone(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def coerceBool(value, default=False):
    if isinstance(value, bool):
        return value

    text = str(value or "").strip().lower()
    if text in ["true", "1", "yes", "on"]:
        return True
    if text in ["false", "0", "no", "off"]:
        return False
    return bool(default)
