from Otto_API.Common.HttpHelpers import httpGet
from Otto_API.Common.HttpHelpers import jsonHeaders
from Otto_API.Common.ParseHelpers import parseMissionResults
from Otto_API.Common.TagIO import getApiBaseUrl
from Otto_API.Missions.QueryHelpers import buildMissionsUrl


def getMissions(logger, debug, mission_status=None, limit=None):
    """
    Get mission status info from OTTO for one or more mission statuses.
    If mission_status is None, returns an empty list (intentional safety).
    """
    try:
        if not mission_status:
            if debug:
                logger.warn("getMissions called with no mission_status")
            return []

        base = getApiBaseUrl()
        url = buildMissionsUrl(base, mission_status, limit)
        if isinstance(mission_status, (list, tuple)):
            statusLabel = ",".join([str(x) for x in mission_status])
        else:
            statusLabel = str(mission_status)

        if debug:
            logger.debug(
                "Otto API - Requesting missions status={} url={}".format(
                    statusLabel, url
                )
            )

        response = httpGet(url=url, headerValues=jsonHeaders())
        results = parseMissionResults(response)

        if debug:
            logger.debug(
                "Otto API - Received {} missions for status {}".format(
                    len(results), statusLabel
                )
            )

        return results

    except Exception as e:
        logger.error(
            "Otto API - Error fetching missions (status={}): {}".format(
                mission_status, e
            )
        )
        return []
