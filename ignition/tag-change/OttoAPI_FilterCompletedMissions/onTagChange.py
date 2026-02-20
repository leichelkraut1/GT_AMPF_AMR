def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	"""Handle Ignition tag-change event for this resource."""
	from java.util import Date
	
	logger = system.util.getLogger("Otto_MissionFilter")
	
	COMPLETED_PATH = "[Otto_FleetManager]Missions/Completed"
	LAST_TS_TAG = "[Otto_FleetManager]Missions/LastCompleteTS"
	
	try:
	    existingTags = system.tag.browse(COMPLETED_PATH).getResults()
	
	    latestTs = None
	
	    for t in existingTags:
	        # Only look at mission UDT instances
	        if str(t.get("tagType")) != "UdtInstance":
	            continue
	
	        execEndPath = str(t.get("fullPath")) + "/execution_end"
	        qv = system.tag.read(execEndPath)
	        execEndVal = qv.value
	
	        if not execEndVal:
	            continue
	
	        # Execution_End may already be a Date, or may be a string
	        if isinstance(execEndVal, Date):
	            ts = execEndVal
	        else:
	            try:
	                ts = system.date.parse(str(execEndVal))
	            except:
	                continue
	
	        if latestTs is None or ts.after(latestTs):
	            latestTs = ts
	
	    if latestTs is not None:
	        system.tag.writeBlocking([LAST_TS_TAG], [latestTs])
	        logger.info(
	            "filterCompletedMissions set LastCompleteTS = {}".format(
	                system.date.format(latestTs, "yyyy-MM-dd HH:mm:ss.SSS")
	            )
	        )
	    else:
	        logger.warn("filterCompletedMissions: No valid Execution_End timestamps found.")
	
	except Exception as e:
	    logger.error("filterCompletedMissions FAILED: {}".format(e))
