const url = require('url');
const async_https_request = require('./async_https_request');

function create_bucket(auth, accountId, bucketName, bucketType) {
  const host = url.parse(auth.apiUrl).hostname;
  const postData = JSON.stringify({
		'accountId': accountId,
		'bucketName': bucketName,
		'bucketType': bucketType
  });
  const options = {
    host: host,
    path: '/b2api/v1/b2_create_bucket',
    method: 'POST',
    headers: {
      'Authorization': auth.authorizationToken,
			'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': postData.length
    }
  };
  return async_https_request(options, postData, false)
};

module.exports = create_bucket;
