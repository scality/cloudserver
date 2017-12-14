var https = require('https');
const async_https_request = require('./async_https_request');

function authorize_account(accountId, applicationKey, host) {
	const options = {
		host: host,
		path: '/b2api/v1/b2_authorize_account',
		auth: accountId + ':' + applicationKey
	};
	return async_https_request(options, null, false);
};

module.exports = authorize_account;
