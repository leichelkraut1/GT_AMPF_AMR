import time
import uuid

from Otto_API.ResultHelpers import buildOperationResult
from Otto_API.TagHelpers import readOptionalTagValue


def memoryTagDef(name, dataType, value=None):
    tagDef = {
        "name": name,
        "tagType": "AtomicTag",
        "valueSource": "memory",
        "dataType": dataType,
    }
    if value is not None:
        tagDef["value"] = value
    return tagDef


def splitTagPath(path):
    if "/" in path:
        return path.rsplit("/", 1)

    if "]" in path:
        providerPath, childName = path.split("]", 1)
        return providerPath + "]", childName

    raise ValueError("Unsupported tag path: {}".format(path))


def ensureFolder(path):
    parentPath, name = splitTagPath(path)
    system.tag.configure(
        parentPath,
        [{"name": name, "tagType": "Folder"}],
        "a"
    )


def defaultCommandState():
    return {
        "latched": False,
        "state": "idle",
        "attempt_count": 0,
        "last_attempt_epoch_ms": 0,
        "last_attempt_ts": "",
        "last_result": "",
        "command_id": "",
    }


def toBool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ["true", "1", "yes", "on"]


def toInt(value, defaultValue=0):
    try:
        return int(value)
    except Exception:
        return defaultValue


def normalizeCommandState(rawState):
    state = defaultCommandState()
    rawState = dict(rawState or {})
    state["latched"] = toBool(rawState.get("latched"))
    state["state"] = str(rawState.get("state") or "idle")
    state["attempt_count"] = toInt(rawState.get("attempt_count"), 0)
    state["last_attempt_epoch_ms"] = toInt(rawState.get("last_attempt_epoch_ms"), 0)
    state["last_attempt_ts"] = str(rawState.get("last_attempt_ts") or "")
    state["last_result"] = str(rawState.get("last_result") or "")
    state["command_id"] = str(rawState.get("command_id") or "")
    return state


def commandStatePaths(commandPath):
    return {
        "request": commandPath + "/Request",
        "enabled": commandPath + "/Enabled",
        "command_type": commandPath + "/CommandType",
        "template_tag_path": commandPath + "/TemplateTagPath",
        "robot_id_tag_path": commandPath + "/RobotIdTagPath",
        "robot_name": commandPath + "/RobotName",
        "mission_name": commandPath + "/MissionName",
        "retry_delay_ms": commandPath + "/RetryDelayMs",
        "max_attempts": commandPath + "/MaxAttempts",
        "latched": commandPath + "/Latched",
        "state": commandPath + "/State",
        "attempt_count": commandPath + "/AttemptCount",
        "last_attempt_epoch_ms": commandPath + "/LastAttemptEpochMs",
        "last_attempt_ts": commandPath + "/LastAttemptTs",
        "last_result": commandPath + "/LastResult",
        "command_id": commandPath + "/CommandId",
    }


def ensureCommandTags(config):
    commandPath = config["command_path"]
    commandsPath = "[Otto_FleetManager]Commands"
    commandGroupPath = config.get("command_group_path", commandsPath + "/Missions")

    ensureFolder(commandsPath)
    ensureFolder(commandGroupPath)
    ensureFolder(commandPath)

    system.tag.configure(
        commandPath,
        [
            memoryTagDef("Request", "Boolean", False),
            memoryTagDef("Enabled", "Boolean", True),
            memoryTagDef("CommandType", "String", config.get("command_type", "")),
            memoryTagDef("TemplateTagPath", "String", config.get("template_tag_path", "")),
            memoryTagDef("RobotIdTagPath", "String", config.get("robot_id_tag_path", "")),
            memoryTagDef("RobotName", "String", config.get("robot_name", "")),
            memoryTagDef("MissionName", "String", config.get("mission_name", "")),
            memoryTagDef("RetryDelayMs", "Int4", config.get("retry_delay_ms", 5000)),
            memoryTagDef("MaxAttempts", "Int4", config.get("max_attempts", 3)),
            memoryTagDef("Latched", "Boolean", False),
            memoryTagDef("State", "String", "idle"),
            memoryTagDef("AttemptCount", "Int4", 0),
            memoryTagDef("LastAttemptEpochMs", "Int8", 0),
            memoryTagDef("LastAttemptTs", "String", ""),
            memoryTagDef("LastResult", "String", ""),
            memoryTagDef("CommandId", "String", ""),
        ],
        "a"
    )


def readCommandState(config):
    paths = commandStatePaths(config["command_path"])
    reads = system.tag.readBlocking([
        paths["latched"],
        paths["state"],
        paths["attempt_count"],
        paths["last_attempt_epoch_ms"],
        paths["last_attempt_ts"],
        paths["last_result"],
        paths["command_id"],
    ])

    return normalizeCommandState({
        "latched": reads[0].value if reads[0].quality.isGood() else False,
        "state": reads[1].value if reads[1].quality.isGood() else "idle",
        "attempt_count": reads[2].value if reads[2].quality.isGood() else 0,
        "last_attempt_epoch_ms": reads[3].value if reads[3].quality.isGood() else 0,
        "last_attempt_ts": reads[4].value if reads[4].quality.isGood() else "",
        "last_result": reads[5].value if reads[5].quality.isGood() else "",
        "command_id": reads[6].value if reads[6].quality.isGood() else "",
    })


def writeCommandState(config, state):
    state = normalizeCommandState(state)
    paths = commandStatePaths(config["command_path"])
    system.tag.writeBlocking(
        [
            paths["latched"],
            paths["state"],
            paths["attempt_count"],
            paths["last_attempt_epoch_ms"],
            paths["last_attempt_ts"],
            paths["last_result"],
            paths["command_id"],
        ],
        [
            state["latched"],
            state["state"],
            state["attempt_count"],
            state["last_attempt_epoch_ms"],
            state["last_attempt_ts"],
            state["last_result"],
            state["command_id"],
        ]
    )


def timestampString(nowEpochMs):
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(float(nowEpochMs) / 1000.0)
    )


def readCommandConfigValues(config):
    paths = commandStatePaths(config["command_path"])
    templateTagPath = readOptionalTagValue(
        paths["template_tag_path"],
        config.get("template_tag_path", "")
    )
    robotIdTagPath = readOptionalTagValue(
        paths["robot_id_tag_path"],
        config.get("robot_id_tag_path", "")
    )
    retryDelayMs = toInt(
        readOptionalTagValue(
            paths["retry_delay_ms"],
            config.get("retry_delay_ms", 5000)
        ),
        config.get("retry_delay_ms", 5000)
    )
    maxAttempts = toInt(
        readOptionalTagValue(
            paths["max_attempts"],
            config.get("max_attempts", 3)
        ),
        config.get("max_attempts", 3)
    )
    return {
        "request": toBool(readOptionalTagValue(paths["request"], config.get("request_default", False))),
        "enabled": toBool(readOptionalTagValue(paths["enabled"], True)),
        "command_type": str(readOptionalTagValue(paths["command_type"], config.get("command_type", "")) or ""),
        "template_tag_path": str(templateTagPath or ""),
        "robot_id_tag_path": str(robotIdTagPath or ""),
        "robot_name": str(readOptionalTagValue(paths["robot_name"], config.get("robot_name", "")) or ""),
        "mission_name": str(readOptionalTagValue(paths["mission_name"], config.get("mission_name", "")) or ""),
        "retry_delay_ms": retryDelayMs,
        "max_attempts": maxAttempts,
    }


def decideCommandAction(requestActive, enabled, currentState, nowEpochMs, retryDelayMs, maxAttempts, uuidFactory=None):
    currentState = normalizeCommandState(currentState)

    if uuidFactory is None:
        uuidFactory = uuid.uuid4

    if not enabled:
        resetState = defaultCommandState()
        if currentState != resetState:
            return ("disabled", resetState)
        return ("idle", resetState)

    if not requestActive:
        resetState = defaultCommandState()
        if currentState != resetState:
            return ("reset", resetState)
        return ("idle", resetState)

    if not currentState["latched"]:
        nextState = dict(currentState)
        nextState["latched"] = True
        nextState["state"] = "pending"
        nextState["attempt_count"] = 1
        nextState["last_attempt_epoch_ms"] = nowEpochMs
        nextState["last_attempt_ts"] = timestampString(nowEpochMs)
        nextState["last_result"] = "pending first execution"
        nextState["command_id"] = str(uuidFactory())
        return ("execute", nextState)

    if (
        currentState["state"] == "failed"
        and currentState["attempt_count"] < maxAttempts
        and (nowEpochMs - currentState["last_attempt_epoch_ms"]) >= retryDelayMs
    ):
        retryState = dict(currentState)
        retryState["state"] = "pending"
        retryState["attempt_count"] = currentState["attempt_count"] + 1
        retryState["last_attempt_epoch_ms"] = nowEpochMs
        retryState["last_attempt_ts"] = timestampString(nowEpochMs)
        retryState["last_result"] = "pending retry"
        return ("retry", retryState)

    return ("hold", currentState)


def buildLoopResult(ok, level, message, action, commandName, commandState, commandConfig=None, commandResult=None):
    return buildOperationResult(
        ok,
        level,
        message,
        data={
            "action": action,
            "command_name": commandName,
            "command_state": commandState,
            "command_config": commandConfig or {},
            "command_result": commandResult,
        },
        action=action,
        command_name=commandName,
        command_state=commandState,
        command_config=commandConfig or {},
        command_result=commandResult,
    )
