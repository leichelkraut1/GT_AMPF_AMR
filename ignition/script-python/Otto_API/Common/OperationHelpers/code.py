from Otto_API.Common.ResultHelpers import buildOperationResult
from Otto_API.Common.TagIO import writeLastSystemResponse
from Otto_API.Common.TagIO import writeLastTriggerResponse


def buildDataResult(ok, level, message, **data):
    """Build a lightweight standard result shape with module-specific fields under data."""
    return buildOperationResult(
        ok,
        level,
        message,
        data=dict(data or {}),
    )


def logAndWriteOperationResult(result, logger):
    """Write shared response side effects and log one operation result."""
    result = dict(result or {})
    data = dict(result.get("data") or {})
    responseText = data.get("response_text")
    responseTexts = list(data.get("response_texts") or [])
    if responseText is None and responseTexts:
        responseText = responseTexts[-1]

    if responseText is not None:
        writeLastSystemResponse(responseText, asyncWrite=True)

    level = str(result.get("level") or "").lower()
    message = str(result.get("message") or "")
    if level == "info":
        logger.info(message)
    elif level == "warn":
        logger.warn(message)
    else:
        logger.error(message)

    writeLastTriggerResponse(message, asyncWrite=True)
    return result
