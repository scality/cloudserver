import services from '../services';
import utils from '../utils';

/**
 * PUT part of object during a multipart upload. Steps include:
 * validating metadata for authorization, bucket existence
 * and multipart upload initiation existence,
 * store object data in datastore upon successful authorization,
 * store object location returned by datastore in metadata and
 * return the result in final cb
 *
 * @param  {string}   accessKey - user access key
 * @param  {metastore}   metastore - metadata storage endpoint
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} log - Werelogs logger
 * @param  {function} cb  - final callback to call with the result
 * @return {function} calls cb from router
 * with err and result as arguments
 */
export default function objectPutPart(accessKey, metastore, request, log, cb) {
    const bucketName = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    const contentMD5 = request.calculatedMD5;

    const size = request.lowerCaseHeaders['content-length'];
    const partNumber = Number.parseInt(request.query.partNumber, 10);
    // AWS caps partNumbers at 10,000
    if (partNumber > 10000) {
        return cb('TooManyParts');
    }
    if (!Number.isInteger(partNumber)) {
        return cb('InvalidArgument');
    }
    // If part size is greater than 5GB, reject it
    if (Number.parseInt(size, 10) > 5368709120) {
        return cb('EntityTooLarge');
    }
    // Note: Parts are supposed to be at least 5MB except for last part.
    // However, there is no way to know whether a part is the last part
    // since keep taking parts until get a completion request.  But can
    // expect parts of at least 5MB until last part.  Also, we check that
    // part sizes are large enough when mutlipart upload completed.

    // Note that keys in the query object retain their case, so
    // request.query.uploadId must be called with that exact
    // capitalization
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        accessKey,
        bucketName,
        objectKey,
        metastore,
        uploadId,
        requestType: 'putPart or complete',
        log,
    };
    const mdParams = {
        partNumber,
        contentMD5,
        size,
        uploadId,
    };
    const objectKeyContext = {
        bucketName,
        owner: accessKey,
        namespace: request.namespace,
    };

    services.metadataValidateMultipart(metadataValParams, (err, mpuBucket) => {
        if (err) {
            return cb(err);
        }
        return services.dataStore(
            null, objectKeyContext, request.post,
            (err, extraArg, keys) => {
                if (err) {
                    return cb(err);
                }
                services.metadataStorePart(mpuBucket.name, keys, mdParams, log,
						cb);
            });
    });
}
