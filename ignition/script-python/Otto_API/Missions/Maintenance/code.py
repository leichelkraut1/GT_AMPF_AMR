from java.util import Date

from Otto_API.Common.TagIO import deleteTagPath
from Otto_API.Common.TagIO import readTagValues
from Otto_API.Common.TimeHelpers import parseIsoTimestampToEpochMillis
from Otto_API.Missions.Runtime import record_removed_mission_if_needed


def parse_date(val):
    """
    Safely parses Ignition Date or string timestamps.
    """
    if val is None:
        return None

    if hasattr(val, "before"):
        return val

    text = str(val).strip()

    if "T" in text and (text.endswith("Z") or "+" in text[10:] or "-" in text[10:]):
        try:
            return Date(parseIsoTimestampToEpochMillis(text))
        except Exception:
            pass

    try:
        return system.date.parse(text)
    except Exception:
        return None


def should_remove_completed_by_age(createdDate, cutoff):
    """
    Return True when a completed mission is older than the retention cutoff.
    """
    return bool(createdDate and cutoff and createdDate.before(cutoff))


def compute_completed_overflow(enrichedRows, maxCompleted, nowDate):
    """
    Return the oldest completed rows that exceed the retention count.
    """
    def sort_key(item):
        return item[2] if item[2] else nowDate

    enriched_sorted = sorted(list(enrichedRows or []), key=sort_key)
    if len(enriched_sorted) <= maxCompleted:
        return []
    return enriched_sorted[:-maxCompleted]


def remove_instance(path, logger=None, debug=False, reason=None):
    """
    Delete a UDT instance.
    """
    try:
        deleteTagPath(path)
        if logger and debug:
            if reason:
                logger.info("Deleted {} ({})".format(path, reason))
            else:
                logger.info("Deleted {}".format(path))
    except Exception as exc:
        if logger:
            if reason:
                logger.warn(
                    "Failed to delete {} ({}): {}".format(
                        path,
                        reason,
                        str(exc)
                    )
                )
            else:
                logger.warn(
                    "Failed to delete {}: {}".format(
                        path,
                        str(exc)
                    )
                )


def cleanup_terminal_folder(folderPath, retentionDays, maxCount, label, logger, browseMissionInstances, debug=False, protectedPaths=None):
    """
    Enforce terminal mission retention and max count for the given folder.
    """
    now = system.date.now()
    cutoff = system.date.addDays(now, -retentionDays)
    protectedPaths = set(protectedPaths or [])

    instances = browseMissionInstances(folderPath)
    removed = []
    removedPaths = set()
    enriched = []

    readPaths = [fullPath + "/Created" for fullPath, _ in instances]
    readResults = []
    if readPaths:
        readResults = readTagValues(readPaths)

    for index, instance in enumerate(instances):
        fullPath, name = instance
        qualifiedValue = readResults[index]
        if not qualifiedValue.quality.isGood():
            logger.warn(
                "Skipping {} mission {} during cleanup - Created tag is not readable".format(
                    label,
                    fullPath
                )
            )
            continue

        createdDate = parse_date(qualifiedValue.value)
        if createdDate is None:
            logger.warn(
                "Skipping {} mission {} during cleanup - invalid Created value".format(
                    label,
                    fullPath
                )
            )
            continue
        enriched.append((fullPath, name, createdDate))

    removed_age = 0
    remaining = []
    for fullPath, name, createdDate in enriched:
        if fullPath in protectedPaths:
            remaining.append((fullPath, name, createdDate))
            continue
        if should_remove_completed_by_age(createdDate, cutoff):
            remove_instance(
                fullPath,
                logger,
                debug,
                "older than {} days".format(retentionDays)
            )
            removed_age += 1
            removedPaths.add(fullPath)
            removed.append((fullPath, "age"))
        else:
            remaining.append((fullPath, name, createdDate))

    if debug:
        logger.info("{} cleanup: removed {} by age".format(label.title(), removed_age))

    excess = compute_completed_overflow(remaining, maxCount, now)
    for fullPath, name, ts in excess:
        if fullPath in removedPaths:
            continue
        remove_instance(
            fullPath,
            logger,
            debug,
            "pruned to max {}".format(maxCount)
        )
        removedPaths.add(fullPath)
        removed.append((fullPath, "max"))

    if debug and excess:
        logger.info(
            "{} cleanup: pruned {} to max {}".format(
                label.title(),
                len(excess),
                maxCount
            )
        )

    return removed


def cleanup_stale_bucket(folderPath, wantedPaths, label, logger, browseMissionInstances, debug=False, nowTimestamp=None):
    """
    Remove mission instances in folderPath that are not present in wantedPaths.
    """
    removed = []
    wantedPaths = set(wantedPaths or [])

    for fullPath, name in browseMissionInstances(folderPath):
        if fullPath in wantedPaths:
            continue
        if nowTimestamp is not None:
            record_removed_mission_if_needed(fullPath, folderPath, nowTimestamp, logger=logger)
        remove_instance(
            fullPath,
            logger,
            debug,
            "stale (not returned)"
        )
        removed.append((fullPath, "stale_{}".format(label)))

    return removed
