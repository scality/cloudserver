const url = require('url');
const async_https_request = require('./async_https_request');

function finish_large_file(auth, uploadId, sha1Array) {
  var parsed_url = url.parse(auth.apiUrl);
  data = JSON.stringify({
    'fileId': uploadId,
    'partSha1Array': sha1Array
  });
  const options = {
    host: parsed_url.hostname,
    path: '/b2api/v1/b2_finish_large_file',
    method: 'POST',
    headers: {
      'Authorization': auth.authorizationToken,
      'Content-length': data.length
    }
  };
  return async_https_request(options, data, false)
};

module.exports = finish_large_file;
