def doGet(request, session):
	import json
	
	url=system.tag.read("[default]AMR_FM/AMR_FM_URL").value + "api/fleet/v2/system/state/"
		
	response = system.net.httpGet(url)
	system.tag.write("[default]AMR_FM/testResponse",response)
		
	return {'html': '<html><body>Hello World</body></html>'}