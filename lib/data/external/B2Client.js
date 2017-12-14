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

// path = /lib/data/external/b2_lib/
const get_upload_url = require('./b2_lib/b2_get_upload_url');
const upload_file = require('./b2_lib/b2_upload_file');
const SHA1Sum = require('./b2_lib/b2_sha1sum');
const set_auth_and_bucket_id_once = require('./b2_lib/b2_set_auth_and_bucket_id_once');

// Not implemented methods because of non existing equivalent on B2
// objectPutTagging, objectDeleteTagging, copyObject, uploadPartCopy

class B2Client {
	constructor(config) {
		this.b2StorageEndpoint = config.b2StorageEndpoint;
		this.b2StorageCredentials = config.b2StorageCredentials;
		this._b2BucketName = config.b2BucketName;
		this._dataStoreName = config.dataStoreName;
		this._bucketMatch = config.bucketMatch;
	}

	async put(stream, size, keyContext, reqUids, callback) {
		const log = createLogger(reqUids);
		let err = null;
		let final_result = [];
		try {
			await set_auth_and_bucket_id_once(this);
			let result = await get_upload_url(this.auth, this.bucketId);
			let fileName = keyContext.objectKey;
			let hashedStream = new SHA1Sum();
			stream.pipe(hashedStream);
			// When sending the SHA1 checksum at the end,
			// size should size of the file plus the 40 bytes of hex checksum.
			result = await upload_file(result, hashedStream, fileName, size + 40)
			final_result = [fileName, result.fileId];
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'err from data backend',
				err, this._dataStoreName);
		} finally {
			callback(err, final_result[0], final_result[1]);
		}
	}
}

module.exports = B2Client;
