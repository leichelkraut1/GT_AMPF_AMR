from Otto_API.Common.TagIO import browseTagResults


def childRowNames(basePath):
    """
    Return child folder or UDT-instance names under one collection path.
    """
    names = []
    for row in list(browseTagResults(basePath) or []):
        tagType = str(row.get("tagType") or "").strip().lower()
        if tagType in ["folder", "udtinstance"]:
            names.append(str(row.get("name") or ""))
    return sorted(names)
