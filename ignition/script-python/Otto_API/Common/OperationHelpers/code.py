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
