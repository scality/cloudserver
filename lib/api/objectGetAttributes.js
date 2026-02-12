const { promisify } = require('util');
const xml2js = require('xml2js');
const { errors } = require('arsenal');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const parseAttributesHeaders = require('./apiUtils/object/parseAttributesHeader');
const { decodeVersionId, getVersionIdResHeader } = require('./apiUtils/object/versioning');
const { checkExpectedBucketOwner } = require('./apiUtils/authorization/bucketOwner');
const { pushMetric } = require('../utapi/utilities');
const { getPartCountFromMd5 } = require('./apiUtils/object/partInfo');

const checkExpectedBucketOwnerPromise = promisify(checkExpectedBucketOwner);
const validateBucketAndObj = promisify(standardMetadataValidateBucketAndObj);

const OBJECT_GET_ATTRIBUTES = 'objectGetAttributes';
const ATTRIBUTE_HANDLERS = {
  ETag: objMD => objMD['content-md5'],
  ObjectParts: objMD => {
    const partCount = getPartCountFromMd5(objMD);
    return partCount ? { PartsCount: partCount } : undefined;
  },
  StorageClass: objMD => objMD['x-amz-storage-class'],
  ObjectSize: objMD => objMD['content-length'],
};

/**
 * buildXmlResponse - Build XML response for GetObjectAttributes
 * @param {object} objMD - object metadata
 * @param {Set<string>} requestedAttrs - set of requested attribute names
 * @returns {string} XML response
 */
function buildXmlResponse(objMD, requestedAttrs) {
  const attrResp = {};

  for (const [attr, handler] of Object.entries(ATTRIBUTE_HANDLERS)) {
    if (requestedAttrs.has(attr)) {
      const value = handler(objMD);
      if (value !== undefined) {
        attrResp[attr] = value;
      }
    }
  }

  const builder = new xml2js.Builder();
  return builder.buildObject({ GetObjectAttributesResponse: attrResp });
}

/**
 * objectGetAttributes - Retrieves all metadata from an object without returning the object itself
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback optional to keep backward compatibility
 * @returns {Promise<object>} - { xml, responseHeaders }
 * @throws {ArsenalError} NoSuchVersion - if versionId specified but not found
 * @throws {ArsenalError} NoSuchKey - if object not found
 * @throws {ArsenalError} MethodNotAllowed - if object is a delete marker
 */
async function objectGetAttributes(authInfo, request, log, callback) {
  if (callback) {
    return objectGetAttributes(authInfo, request, log)
      .then(result => callback(null, result.xml, result.responseHeaders))
      .catch(err => callback(err, null, err.responseHeaders ?? {}));
  }

  log.trace('processing request', { method: OBJECT_GET_ATTRIBUTES });
  const { bucketName, objectKey, headers, actionImplicitDenies } = request;

  const versionId = decodeVersionId(request.query);
  if (versionId instanceof Error) {
    log.debug('invalid versionId query', {
      method: OBJECT_GET_ATTRIBUTES,
      versionId: request.query.versionId,
      error: versionId,
    });
    throw versionId;
  }

  const metadataValParams = {
    authInfo,
    bucketName,
    objectKey,
    versionId,
    getDeleteMarker: true,
    requestType: request.apiMethods || OBJECT_GET_ATTRIBUTES,
    request,
  };

  let bucket, objectMD;
  try {
    ({ bucket, objectMD } = await validateBucketAndObj(metadataValParams, actionImplicitDenies, log));
    await checkExpectedBucketOwnerPromise(headers, bucket, log);
  } catch (err) {
    log.debug('error validating bucket and object', {
      method: OBJECT_GET_ATTRIBUTES,
      bucket: bucketName,
      key: objectKey,
      versionId,
      error: err,
    });
    throw err;
  }

  const responseHeaders = collectCorsHeaders(headers.origin, request.method, bucket);

  if (!objectMD) {
    log.debug('object not found', {
      method: OBJECT_GET_ATTRIBUTES,
      bucket: bucketName,
      key: objectKey,
      versionId,
    });
    const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
    err.responseHeaders = responseHeaders;
    throw err;
  }

  responseHeaders['x-amz-version-id'] = getVersionIdResHeader(bucket.getVersioningConfiguration(), objectMD);
  responseHeaders['Last-Modified'] = objectMD['last-modified'] && new Date(objectMD['last-modified']).toUTCString();

  if (objectMD.isDeleteMarker) {
    log.debug('attempt to get attributes of a delete marker', {
      method: OBJECT_GET_ATTRIBUTES,
      bucket: bucketName,
      key: objectKey,
      versionId,
    });
    responseHeaders['x-amz-delete-marker'] = true;
    const err = errors.MethodNotAllowed;
    err.responseHeaders = responseHeaders;
    throw err;
  }

  const requestedAttrs = parseAttributesHeaders(headers);

  if (requestedAttrs.has('Checksum')) {
    log.debug('Checksum attribute requested but not implemented', {
      method: OBJECT_GET_ATTRIBUTES,
      bucket: bucketName,
      key: objectKey,
      versionId,
    });
    const err = errors.NotImplemented.customizeDescription('Checksum attribute is not implemented');
    err.responseHeaders = responseHeaders;
    throw err;
  }

  pushMetric(OBJECT_GET_ATTRIBUTES, log, {
    authInfo,
    bucket: bucketName,
    keys: [objectKey],
    versionId: objectMD?.versionId,
    location: objectMD?.dataStoreName,
  });

  const xml = buildXmlResponse(objectMD, requestedAttrs);
  return { xml, responseHeaders };
}

module.exports = objectGetAttributes;
