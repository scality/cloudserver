const url = require('url');
const async_https_request = require('./async_https_request');

function get_file_info(auth, fileId) {
	const params = url.parse(auth.apiUrl);
	const options = {
		host: params.hostname,
		path: '/b2api/v1/b2_get_file_info?fileId=' + fileId,
		method: 'GET',
		headers: {
			'Authorization': auth.authorizationToken,
		}
	};
	return async_https_request(options, null, false);
}

module.exports = get_file_info;
