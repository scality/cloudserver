const url = require('url');
const async_https_request = require('./async_https_request');

function upload_part(auth, stream, partNumber, size) {
  var parsed_url = url.parse(auth.uploadUrl);
  const options = {
    host: parsed_url.hostname,
    path: parsed_url.pathname,
    method: 'POST',
    headers: {
      'Authorization': auth.authorizationToken,
      'X-Bz-Part-Number' : partNumber,
      'Content-length': size,
      'X-Bz-Content-Sha1': 'hex_digits_at_end'
    }
  };
  return async_https_request(options, stream, false)
};

module.exports = upload_part;
