const url = require('url');
const async_https_request = require('./async_https_request');

function upload_file(auth, stream, fileName, size) {
	const options = {
		host: url.parse(auth.uploadUrl).hostname,
		path: url.parse(auth.uploadUrl).pathname,
		method: 'POST',
		headers: {
			'Authorization': auth.authorizationToken,
			'X-Bz-File-Name' : fileName,
			'Content-Type': 'text/plain',
			'Content-length': size,
			'X-Bz-Content-Sha1': 'hex_digits_at_end'
		}
	};
	return async_https_request(options, stream, false);
};

module.exports = upload_file;
