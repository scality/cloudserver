// External imports
const { s3middleware } = require('arsenal');
const async = require('async');
const MD5Sum = s3middleware.MD5Sum;

// path = /lib/
const { config } = require('../../Config');
const { prepareStream } = require('../../api/apiUtils/object/prepareStream');
const { validateAndFilterMpuParts } =
	require('../../api/apiUtils/object/processMpuParts');

// path = /lib/data/
const createLogger = require('../multipleBackendLogger');

// path = /lib/data/external/
const { logHelper } = require('./utils');

class B2Client {
	constructor(config) {
		this.b2StorageEndpoint = config.b2StorageEndpoint;
		this.b2StorageCredentials = config.b2StorageCredentials;
		this._b2BucketName = config.b2BucketName;
		this._dataStoreName = config.dataStoreName;
		this._bucketMatch = config.bucketMatch;
	}
}

module.exports = B2Client;
