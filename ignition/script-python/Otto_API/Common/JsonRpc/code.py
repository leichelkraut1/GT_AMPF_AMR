def postJsonRpcPayload(operationsUrl, payload, postFunc):
    return postFunc(
        url=operationsUrl,
        postData=system.util.jsonEncode(payload),
    )
