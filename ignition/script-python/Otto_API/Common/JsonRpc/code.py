import json
import time


class JsonRpcResponse(object):
    def __init__(self, responseText, payload=None, parseError=""):
        self.response_text = responseText
        self.payload = payload
        self.parse_error = str(parseError or "")
        payloadDict = payload if isinstance(payload, dict) else {}
        self.has_result = "result" in payloadDict
        self.result = payloadDict.get("result") if self.has_result else None
        self.has_error = "error" in payloadDict
        self.error = payloadDict.get("error") if self.has_error else None
        self.is_unexpected = (
            not self.parse_error
            and not self.has_result
            and not self.has_error
        )

    def errorText(self):
        return json.dumps(self.error)


def buildJsonRpcPayload(method, params, nowEpoch=None):
    if nowEpoch is None:
        nowEpoch = time.time()

    return {
        "id": int(nowEpoch),
        "jsonrpc": "2.0",
        "method": method,
        "params": dict(params or {}),
    }


def parseJsonRpcResponse(responseText):
    try:
        return JsonRpcResponse(responseText, json.loads(responseText))
    except Exception as exc:
        return JsonRpcResponse(responseText, parseError=str(exc))


def interpretJsonRpcMutationResponse(
    responseText,
    successMessage,
    nonJsonPrefix,
    apiErrorPrefix,
    unexpectedPrefix,
):
    response = parseJsonRpcResponse(responseText)
    if response.parse_error:
        return ("error", "{}: {}".format(nonJsonPrefix, response.parse_error))
    if response.has_result:
        return ("info", successMessage)
    if response.has_error:
        return ("warn", "{}: {}".format(apiErrorPrefix, response.errorText()))
    return ("warn", "{}: {}".format(unexpectedPrefix, responseText))


def postJsonRpcPayload(operationsUrl, payload, postFunc):
    return postFunc(
        url=operationsUrl,
        postData=system.util.jsonEncode(payload),
    )
