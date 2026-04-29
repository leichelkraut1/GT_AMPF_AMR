def _typedValueToData(value):
    if hasattr(value, "toDict"):
        return value.toDict()
    if isinstance(value, list) or isinstance(value, tuple):
        return [_typedValueToData(item) for item in list(value or [])]
    if isinstance(value, dict):
        return dict(
            (key, _typedValueToData(item))
            for key, item in dict(value or {}).items()
        )
    return value


class OperationHealth(object):
    def __init__(self, ok, level, message, warnings=None, issues=None):
        self.ok = bool(ok)
        self.level = str(level or "").strip()
        self.message = str(message or "")
        self.warnings = list(warnings or [])
        self.issues = list(issues or [])

    def isError(self):
        return str(self.level or "").lower() == "error"

    def isWarn(self):
        return str(self.level or "").lower() == "warn"

    def isHealthy(self):
        return bool(self.ok)

    @classmethod
    def fromDict(cls, result):
        if isinstance(result, cls):
            return result
        result = dict(result or {})
        return cls(
            result.get("ok"),
            result.get("level"),
            result.get("message"),
            warnings=result.get("warnings"),
            issues=result.get("issues"),
        )

    def healthDict(self):
        return {
            "ok": self.ok,
            "level": self.level,
            "message": self.message,
            "warnings": _typedValueToData(self.warnings),
            "issues": _typedValueToData(self.issues),
        }

    def toDict(self):
        return self.healthDict()


class OperationalResult(OperationHealth):
    def __init__(
        self,
        ok,
        level,
        message,
        typedFields=None,
        dataFields=None,
        sharedFields=None,
    ):
        typedFields = dict(typedFields or {})
        dataFields = dict(dataFields or {})
        sharedFields = dict(sharedFields or {})
        OperationHealth.__init__(
            self,
            ok,
            level,
            message,
            warnings=sharedFields.get("warnings"),
            issues=sharedFields.get("issues"),
        )
        self._typed_fields = typedFields
        self._data_fields = dataFields
        self._shared_fields = sharedFields

        dataPayload = {}
        for fieldName, value in list(typedFields.items()):
            dataPayload[fieldName] = _typedValueToData(value)

        for fieldName, value in list(dataFields.items()):
            dataPayload[fieldName] = _typedValueToData(value)

        for fieldName, value in list(sharedFields.items()):
            dataPayload[fieldName] = _typedValueToData(value)

        self.data = dataPayload

    @classmethod
    def fromDict(cls, result):
        if isinstance(result, cls):
            return result
        result = dict(result or {})
        sharedFields = {}
        if "warnings" in result:
            sharedFields["warnings"] = result.get("warnings")
        if "issues" in result:
            sharedFields["issues"] = result.get("issues")
        return cls(
            result.get("ok"),
            result.get("level"),
            result.get("message"),
            dataFields=result.get("data"),
            sharedFields=sharedFields,
        )

    def _serializedSharedFields(self):
        fields = {}
        fields.update(dict(
            (fieldName, _typedValueToData(value))
            for fieldName, value in dict(self._typed_fields or {}).items()
        ))
        fields.update(dict(
            (fieldName, _typedValueToData(value))
            for fieldName, value in dict(self._shared_fields or {}).items()
        ))
        return fields

    def toDict(self):
        result = self.healthDict()
        if "warnings" not in dict(self._shared_fields or {}):
            result.pop("warnings", None)
        if "issues" not in dict(self._shared_fields or {}):
            result.pop("issues", None)
        result["data"] = _typedValueToData(self.data)
        result.update(self._serializedSharedFields())
        return result


class RecordSyncResult(OperationalResult):
    def __init__(
        self,
        ok,
        level,
        message,
        records=None,
        recordsByName=None,
        writes=None,
        value=None,
        dataFields=None,
        sharedFields=None,
    ):
        self.records = list(records or [])
        self.records_by_name = dict(recordsByName or {})
        self.writes = list(writes or [])
        self.value = value

        dataFields = dict(dataFields or {})
        dataFields["value"] = value

        OperationalResult.__init__(
            self,
            ok,
            level,
            message,
            typedFields={
                "records": self.records,
                "records_by_name": self.records_by_name,
                "writes": self.writes,
            },
            dataFields=dataFields,
            sharedFields=sharedFields,
        )
