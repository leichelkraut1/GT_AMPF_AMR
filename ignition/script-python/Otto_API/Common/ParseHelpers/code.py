import json


def parseServerStatus(responseText):
    """
    Parse a Fleet Manager server-state response and return a status string.
    """
    if not responseText:
        raise ValueError("Empty server status response")

    payload = json.loads(responseText)
    return payload.get("state", "Unknown")


def parseMissionResults(responseText):
    """
    Parse a missions response and return the list payload.
    """
    if not responseText:
        return []

    payload = json.loads(responseText)
    results = payload.get("results", [])
    if not isinstance(results, list):
        return []
    return results


def parseJsonResponse(responseText):
    """
    Parse a JSON response body and return the decoded payload.
    """
    if not responseText:
        raise ValueError("Empty JSON response")
    return json.loads(responseText)


def parseListPayload(responseText):
    """
    Parse a JSON response that may be either a list or a dict with results.
    """
    payload = parseJsonResponse(responseText)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        results = payload.get("results", [])
        if isinstance(results, list):
            return results
    return []
