try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit

from Otto_API.Common.TagHelpers import ensureFolder
from Otto_API.Common.TagHelpers import ensureMemoryTag
from Otto_API.Common.TagHelpers import getDisableLogOfMainCycleHttpPath
from Otto_API.Common.TagHelpers import getFleetConfigPath
from Otto_API.Common.TagHelpers import getFleetRootPath
from Otto_API.Common.TagHelpers import getMainCycleEndpointsPath


DEFAULT_MAIN_CYCLE_ENDPOINTS = "\n".join([
    "GET /system/state/",
    "GET /robots/states/",
    "GET /robots/activities/",
    "GET /robots/batteries/",
    "GET /containers/",
    "GET /missions/",
])


def normalizedEndpointKey(method, url):
    method = str(method or "").strip().upper()
    path = str(urlsplit(str(url or "")).path or "/").strip() or "/"
    return "{} {}".format(method, path)


def parseEndpointList(rawValue):
    endpoints = set()
    for line in str(rawValue or "").splitlines():
        entry = str(line or "").strip()
        if not entry:
            continue
        endpoints.add(entry.upper())
    return endpoints


def ensureHttpLogConfigTags():
    ensureFolder(getFleetRootPath())
    ensureFolder(getFleetConfigPath())
    ensureMemoryTag(getDisableLogOfMainCycleHttpPath(), "Boolean", False)
    ensureMemoryTag(getMainCycleEndpointsPath(), "String", DEFAULT_MAIN_CYCLE_ENDPOINTS)
