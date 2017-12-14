const async_https_request = require('./async_https_request');
const url = require('url');

function cancel_large_file(auth, fileId) {
	const host = url.parse(auth.apiUrl).hostname;
	const postData = JSON.stringify({
		'fileId': fileId,
	});
	const options = {
		host: host,
		path: '/b2api/v1/b2_cancel_large_file',
		method: 'POST',
		headers: {
			'Authorization': auth.authorizationToken,
			'Content-Length': postData.length
		}
	};
	return async_https_request(options, postData, false);
};

module.exports = cancel_large_file;
