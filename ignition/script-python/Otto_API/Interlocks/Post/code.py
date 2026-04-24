import time

from Otto_API.Common.HttpHelpers import httpPost
from Otto_API.Common.OperationHelpers import buildDataResult
from Otto_API.Common.TagIO import getOttoOperationsUrl


def _log():
    return system.util.getLogger("Otto_API.Interlocks.Post")


def _buildRpcPayload(interlockId, state, mask):
    return {
        "id": int(time.time() * 1000),
        "jsonrpc": "2.0",
        "method": "setInterlockState",
        "params": {
            "id": str(interlockId),
            "mask": int(mask),
            "state": int(state),
        },
    }


def setInterlockStateFromInputs(interlockId, state, mask, fleetManagerURL, postFunc):
    """
    Set one OTTO interlock state through the shared operations endpoint.
    """
    if not interlockId:
        return buildDataResult(
            False,
            "warn",
            "No interlock id supplied for setInterlockState",
            interlock_id=interlockId,
            state=state,
            mask=mask,
        )

    try:
        payload = _buildRpcPayload(interlockId, state, mask)
        response = postFunc(
            url=fleetManagerURL,
            postData=system.util.jsonEncode(payload),
        )
        responsePayload = system.util.jsonDecode(response)
        if isinstance(responsePayload, dict) and responsePayload.get("error") is not None:
            errorText = responsePayload.get("error")
            return buildDataResult(
                False,
                "error",
                "setInterlockState failed for [{}]: {}".format(interlockId, errorText),
                interlock_id=interlockId,
                state=int(state),
                mask=int(mask),
                response_text=response,
                payload=payload,
            )

        return buildDataResult(
            True,
            "info",
            "setInterlockState queued for [{}] -> {}".format(interlockId, int(state)),
            interlock_id=interlockId,
            state=int(state),
            mask=int(mask),
            response_text=response,
            payload=payload,
        )
    except Exception as exc:
        return buildDataResult(
            False,
            "error",
            "setInterlockState failed for [{}]: {}".format(interlockId, exc),
            interlock_id=interlockId,
            state=state,
            mask=mask,
        )


def setInterlockState(interlockId, state, mask=65535):
    """
    Set one OTTO interlock state using the shared operations endpoint.
    """
    logger = _log()
    logger.info(
        "Setting interlock [{}] state to [{}] with mask [{}]".format(
            interlockId,
            state,
            mask,
        )
    )
    return setInterlockStateFromInputs(
        interlockId,
        state,
        mask,
        getOttoOperationsUrl(),
        httpPost,
    )
