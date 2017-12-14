const async_https_request = require('./async_https_request');
const url = require('url');

// create_multipart_upload is an alias of b2_start_large_file
// see https://www.backblaze.com/b2/docs/b2_start_large_file.html
function create_multipart_upload (auth, data) {
	const host = url.parse(auth.apiUrl).hostname;
	const postData = JSON.stringify({
		'bucketId': data.bucketId,
		'fileName': data.fileName,
		'contentType': 'b2/x-auto'
	});
	const options = {
		host: host,
		path: '/b2api/v1/b2_start_large_file',
		method: 'POST',
		headers: {
			'Authorization': auth.authorizationToken,
			'Content-Length': postData.length,
		}
	};
	return async_https_request(options, postData, false);
};

module.exports = create_multipart_upload;
