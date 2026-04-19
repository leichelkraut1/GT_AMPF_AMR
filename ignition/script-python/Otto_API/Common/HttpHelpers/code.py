def jsonHeaders(extraHeaders=None):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if extraHeaders:
        headers.update(dict(extraHeaders))
    return headers


def httpGet(url, headerValues=None):
    return system.net.httpGet(
        url=url,
        bypassCertValidation=True,
        headerValues=headerValues or jsonHeaders(),
    )


def httpPost(url, postData, contentType="application/json", headerValues=None):
    return system.net.httpPost(
        url=url,
        postData=postData,
        contentType=contentType,
        headerValues=headerValues or {"Accept": "application/json"},
        bypassCertValidation=True,
    )
