const url = require('url');
const async_https_request = require('./async_https_request');

function download_file_by_id(auth, fileId, range) {
	var params = url.parse(auth.apiUrl);
	const options = {
		host: params.hostname,
		path: '/b2api/v1/b2_download_file_by_id?fileId=' + fileId,
		method: 'GET',
		Range: range,
		headers: {
			'Authorization': auth.authorizationToken,
		}
	};
	return async_https_request(options, null, true);
}

module.exports = download_file_by_id;
