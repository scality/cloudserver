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
const delete_file_version = require('./b2_lib/b2_delete_file_version');
const get_upload_url = require('./b2_lib/b2_get_upload_url');
const get_upload_part_url = require('./b2_lib/b2_get_upload_part_url');
const upload_file = require('./b2_lib/b2_upload_file');
const upload_part = require('./b2_lib/b2_upload_part');
const list_parts = require('./b2_lib/b2_list_parts');
const SHA1Sum = require('./b2_lib/b2_sha1sum');
const download_file_by_id = require('./b2_lib/b2_download_file_by_id');
const set_auth_once = require('./b2_lib/b2_set_auth_once');
const create_multipart_upload = require('./b2_lib/b2_create_multipart_upload');
const finish_large_file = require('./b2_lib/b2_finish_large_file');
const cancel_large_file = require('./b2_lib/b2_cancel_large_file');
const get_file_info = require('./b2_lib/b2_get_file_info');
const create_bucket = require('./b2_lib/b2_create_bucket');
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

	async createMPU(Key, metaHeaders, bucketName,
		websiteRedirectHeader, contentType, cacheControl,
		contentDisposition, contentEncoding, log, callback)
		{
		let err = null;
		let mpuResObj = {};
		try {
			await set_auth_and_bucket_id_once(this);
			let data = {
				fileName: Key,
				bucketId: this.bucketId
			}
			let result = await create_multipart_upload(this.auth, data)
			mpuResObj = {
				'Bucket': result.bucketId,
				'Key': data.fileName,
				'UploadId': result.fileId
			}
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'err from data backend',
				err, this._dataStoreName);
		} finally {
			return callback(err, mpuResObj);
		}
	};

	async uploadPart(request, streamingV4Params, stream, size, key, uploadId,
		partNumber, bucketName, log, callback)
	{
		let err = null;
		let result = null;
		let sha1Stream = {};
		try {
			await set_auth_and_bucket_id_once(this);
			let hashedStream = stream;
			if (request) {
				const partStream = prepareStream(
					request, streamingV4Params, log, callback);
				hashedStream = new MD5Sum();
				partStream.pipe(hashedStream);
				sha1Stream = new SHA1Sum();
				hashedStream.pipe(sha1Stream);
			}
			result = await get_upload_part_url(this.auth, uploadId)
			await upload_part(result, sha1Stream, partNumber, size + 40);
			result = {
				'Key': key,
				'dataStoreType': 'b2',
				'dataStoreName': this._dataStoreName,
				'dataStoreETag': hashedStream.completedHash,
				'extraMetadata': sha1Stream.completedHash
			}
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'err from data backend ' +
				'on uploadPart', err, this._dataStoreName);
		} finally {
			callback(err, result);
		}
	}

	async completeMPU(jsonList, mdInfo, key, uploadId, bucketName, log, callback) {
		let err = null;
		let completeObjData = {};
		let mpuError = {};
		try {
			await set_auth_and_bucket_id_once(this);
			const b2Bucket = this.b2BucketName;
			const Key = key;
			const { storedParts, mpuOverviewKey, splitter } = mdInfo;
			const filteredPartsObj = validateAndFilterMpuParts(storedParts,
					jsonList, mpuOverviewKey, splitter, log);
			let partArray = [];
			filteredPartsObj.partList.forEach(part => {
				let etag = part.extraMD;
				partArray.push(etag);
			});
			let completeMpuRes = await finish_large_file(this.auth, uploadId, partArray);
			completeObjData = {
					key: key,
					filteredPartsObj,
					dataStoreVersionId: completeMpuRes.fileId
			};
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'err from data backend ' +
				'on uploadPart', err, this._dataStoreName);
		} finally {
			callback(err, completeObjData);
		}
	}

	async abortMPU(key, uploadId, bucketName, log, callback) {
		let err = null;
		let result = null;
		try {
			await set_auth_and_bucket_id_once(this);
			result = await cancel_large_file(this.auth, uploadId);
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'There was an error aborting ' +
			'the MPU on BackBlaze B2. You should abort directly on B2 ' +
			'using the same uploadId.', err, this._dataStoreName);
		} finally {
			return callback(err, result);
		}
	};

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

	async head(objectGetInfo, reqUids, callback) {
		const log = createLogger(reqUids);
		let err = null;
		let result = null;
		try {
			await set_auth_and_bucket_id_once(this);
			const { dataStoreVersionId } = objectGetInfo;
			result = await get_file_info(this.auth, dataStoreVersionId);
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'err from data backend',
				err, this._dataStoreName);
		} finally {
			callback(err, result)
		}
	}

	//Based on Azure logic: Create bucket if not exist
	async healthcheck(location, callback) {
		const b2Resp = {};
		let err = null;
		let result = null;
		let bucketType = 'allPrivate';
		try {
			await set_auth_once(this);
			result = await create_bucket(this.auth, this.b2StorageCredentials.accountId, this._b2BucketName, bucketType);
		} catch (e) {
			err = e;
		} finally {
			if (err) {
				if (err.code === 400 &&
				err.description === 'duplicate_bucket_name' &&
				err.customizeDescription === 'Bucket name is already in use') {
					b2Resp[location] = { message: 'Congrats! You can access the BackBlaze storage account' };
				} else {
					b2Resp[location] = { error: err.customizeDescription, external: true };
				}
			}
			else {
				b2Resp[location] = { message: 'Congrats! You can access the BackBlaze storage account' };
			}
			callback(null, b2Resp)
		}
	}

	async get(objectGetInfo, range, reqUids, callback) {
		const log = createLogger(reqUids);
		let err = null;
		let result = null;
		try {
			await set_auth_and_bucket_id_once(this);
			const { dataStoreVersionId } = objectGetInfo;
			result = await download_file_by_id(this.auth, dataStoreVersionId, range);
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'error streaming data from B2',
				err, this._dataStoreName);
		} finally {
			callback(err, result);
		}
	}

	async delete(objectGetInfo, reqUids, callback) {
		const log = createLogger(reqUids);
		let err = null;
        let result = null;
		try {
			await set_auth_and_bucket_id_once(this);
			result = await delete_file_version(
				this.auth,
				this.b2StorageEndpoint,
				objectGetInfo.key,
				objectGetInfo.dataStoreVersionId
			);
		} catch (e) {
			err = e;
			logHelper(log, 'error', 'error deleting object from ' +
			'datastore', err, this._dataStoreName);
		} finally {
			callback(err, result);
		}
	}
}

module.exports = B2Client;
