import time

from Otto_API import Post

from MainController.CommandHelpers import buildLoopResult
from MainController.CommandHelpers import decideCommandAction
from MainController.CommandHelpers import ensureCommandTags
from MainController.CommandHelpers import readCommandConfigValues
from MainController.CommandHelpers import readCommandState
from MainController.CommandHelpers import timestampString
from MainController.CommandHelpers import writeCommandState


def executeCreateMission(commandConfig, executeMission=None):
    if executeMission is None:
        executeMission = Post.createMission

    return executeMission(
        templateTagPath=commandConfig["template_tag_path"],
        robotTagPath=commandConfig["robot_id_tag_path"],
        missionName=commandConfig["mission_name"],
    )


def executeFinalizeMission(commandConfig, executeMission=None):
    if executeMission is None:
        executeMission = Post.finalizeMission

    return executeMission(commandConfig["robot_name"])


def executeCommand(commandConfig, executeMission=None):
    commandType = str(commandConfig.get("command_type") or "").strip().lower()

    if commandType == "create_mission":
        return executeCreateMission(commandConfig, executeMission)

    if commandType == "finalize_mission":
        return executeFinalizeMission(commandConfig, executeMission)

    return {
        "ok": False,
        "level": "error",
        "message": "Unsupported command type: {}".format(commandConfig.get("command_type")),
    }


def _executeCommandAction(config, commandConfig, action, state, logger, executeMission):
    state = dict(state)
    state["state"] = "running"
    state["last_result"] = "running"
    writeCommandState(config, state)

    result = executeCommand(commandConfig, executeMission)

    finalState = dict(state)
    finalState["last_attempt_ts"] = finalState["last_attempt_ts"] or timestampString(int(time.time() * 1000))
    finalState["last_result"] = result.get("message", "")
    finalState["state"] = "success" if result.get("ok") else "failed"
    writeCommandState(config, finalState)

    logMethod = logger.info if result.get("ok") else logger.warn
    logMethod(
        "Command [{}] {} - {}".format(
            config["name"],
            finalState["state"],
            result.get("message", "")
        )
    )

    return buildLoopResult(
        result.get("ok", False),
        result.get("level", "info"),
        result.get("message", ""),
        action,
        config["name"],
        finalState,
        commandConfig=commandConfig,
        commandResult=result,
    )


def runConfiguredCommand(config, nowEpochMs=None, uuidFactory=None, executeMission=None):
    logger = system.util.getLogger("MainController_CommandLoop")

    if nowEpochMs is None:
        nowEpochMs = int(time.time() * 1000)

    ensureCommandTags(config)

    commandConfig = readCommandConfigValues(config)
    currentState = readCommandState(config)

    action, nextState = decideCommandAction(
        commandConfig["request"],
        commandConfig["enabled"],
        currentState,
        nowEpochMs,
        commandConfig["retry_delay_ms"],
        commandConfig["max_attempts"],
        uuidFactory,
    )

    writeCommandState(config, nextState)

    if action in ["execute", "retry"]:
        return _executeCommandAction(
            config,
            commandConfig,
            action,
            nextState,
            logger,
            executeMission,
        )

    if action == "reset":
        logger.info("Command [{}] reset after request fell false".format(config["name"]))
    elif action == "disabled":
        logger.info("Command [{}] disabled".format(config["name"]))
    elif action == "hold":
        logger.debug(
            "Command [{}] holding state [{}]".format(
                config["name"],
                nextState["state"]
            )
        )

    return buildLoopResult(
        True,
        "info",
        "Command [{}] action={}".format(config["name"], action),
        action,
        config["name"],
        nextState,
        commandConfig=commandConfig,
    )
