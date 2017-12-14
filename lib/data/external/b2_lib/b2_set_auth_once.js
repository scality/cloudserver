const authorize_account = require('./b2_authorize_account');
const async = require('async');

async function set_auth_once(that) {
	if (that.auth == undefined) {
		that.auth = await authorize_account(that.b2StorageCredentials.accountId,
			that.b2StorageCredentials.b2ApplicationKey,
			that.b2StorageEndpoint);
	};
}

module.exports = set_auth_once;
