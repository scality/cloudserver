var https = require('https');
const async_https_request = require('./async_https_request');

function list_buckets(accountId, token, host, b2BucketName) {
	const postData = JSON.stringify({
		'accountId': accountId,
	});
	const options = {
		host: host,
		path: '/b2api/v1/b2_list_buckets',
		method: 'POST',
		headers: {
			'Authorization': token,
			'Content-Type': 'application/x-www-form-urlencoded',
			'Content-Length': postData.length
		}
	};
	return async_https_request(options, postData, false);
}

module.exports = list_buckets;
