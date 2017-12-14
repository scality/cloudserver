const url = require('url');
const async_https_request = require('./async_https_request');

function list_parts(auth, fileId, startPartNumber, maxPartCount) {
	maxPartCount = maxPartCount > 1000 ? 1000 : maxPartCount;
	var params = url.parse(auth.apiUrl);
	const postData = JSON.stringify({
		'fileId': fileId,
		'startPartNumber': startPartNumber,
		'maxPartCount': maxPartCount
	});
	const options = {
		host: params.hostname,
		path: '/b2api/v1/b2_list_parts',
		method: 'POST',
		headers: {
			'Authorization': auth.authorizationToken,
			'Content-Length': postData.length
		}
	};
	return async_https_request(options, postData, false);
}

module.exports = list_parts;
