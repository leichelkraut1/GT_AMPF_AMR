def buildOperationResult(ok, level, message, data=None, **extra):
    """
    Build a normalized operation result object.

    Common contract:
    - ok
    - level
    - message
    - data

    Module-specific fields may still be included through extra kwargs.
    """
    result = {
        "ok": ok,
        "level": level,
        "message": message,
        "data": data,
    }
    result.update(extra)
    return result
