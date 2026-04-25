from Otto_API.Common.ResultHelpers import buildOperationResult


def buildDataResult(ok, level, message, **data):
    """Build a lightweight standard result shape with module-specific fields under data."""
    return buildOperationResult(
        ok,
        level,
        message,
        data=dict(data or {}),
    )


def logOperationResult(result, logger):
    """Log one operation result and return it unchanged."""
    result = dict(result or {})
    level = str(result.get("level") or "").lower()
    message = str(result.get("message") or "")
    if level == "info":
        logger.info(message)
    elif level == "warn":
        logger.warn(message)
    else:
        logger.error(message)

    return result
