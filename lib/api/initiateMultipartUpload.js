import UUID from 'node-uuid';
import escapeForXML from '../utilities/escapeForXML';
import { pushMetric } from '../utapi/utilities';
import { errors } from 'arsenal';
import { cleanUpBucket } from './apiUtils/bucket/bucketCreation';
import constants from '../../constants';
import services from '../services';
import utils from '../utils';

/*
Sample xml response:
<?xml version='1.0' encoding='UTF-8'?>
<InitiateMultipartUploadResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
  <Bucket>example-bucket</Bucket>
  <Key>example-object</Key>
  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS
  1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
</InitiateMultipartUploadResult>
*/

/**
 * _convertToXml - Convert the `xmlParams` object created in
 * `initiateMultipartUpload()` to an XML DOM string
 * @param {object} xmlParams - The object created in
 * `initiateMultipartUpload()` to convert into an XML DOM string
 * @return {string} xml.join('') - The XML DOM string
 */
const _convertToXml = xmlParams => {
    const xml = [];
    xml.push('<?xml version="1.0" encoding="UTF-8"?>',
             '<InitiateMultipartUploadResult ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
             `<Bucket>${xmlParams.bucketName}</Bucket>`,
             `<Key>${escapeForXML(xmlParams.objectKey)}</Key>`,
             `<UploadId>${xmlParams.uploadId}</UploadId>`,
             '</InitiateMultipartUploadResult>'
    );

    return xml.join('');
};

/**
 * Initiate multipart upload returning xml that includes the UploadId
 * for the multipart upload
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with requester's
 *                               info
 * @param  {request} request - request object given by router,
 * includes normalized headers
 * @param  {object} log - the log request
 * @param  {function} callback - final callback to call with the result
 * @return {undefined} calls callback from router
 * with err and result as arguments
 */
export default
function initiateMultipartUpload(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'initiateMultipartUpload' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    // Note that we are using the string set forth in constants.js
    // to split components in the storage
    // of each MPU.  AWS does not restrict characters in object keys so
    // there is the possiblity that the chosen splitter will occur in the object
    // name itself. To prevent this, we are restricting the creation of a
    // multipart upload object with a key containing the splitter.
    const metaHeaders = utils.getMetaHeaders(request.headers);
    // Generate uniqueID without dashes so that routing not messed up
    const uploadId = UUID.v4().replace(/-/g, '');
    // TODO: Add this as a utility function for all object put requests
    // but after authentication so that string to sign is not impacted
    // This is GH Issue#89
    const storageClassOptions =
        ['standard', 'standard_ia', 'reduced_redundancy'];
    let storageClass = 'STANDARD';
    if (storageClassOptions.indexOf(request
        .headers['x-amz-storage-class']) > -1) {
        storageClass = request.headers['x-amz-storage-class']
            .toUpperCase();
    }
    const metadataValParams = {
        objectKey,
        authInfo,
        bucketName,
        // Required permissions for this action are same as objectPut
        requestType: 'objectPut',
        log,
    };
    const accountCanonicalID = authInfo.getCanonicalID();
    let initiatorID = accountCanonicalID;
    let initiatorDisplayName = authInfo.getAccountDisplayName();
    if (authInfo.isRequesterAnIAMUser()) {
        initiatorID = authInfo.getArn();
        initiatorDisplayName = authInfo.getIAMdisplayName();
    }
    const metadataStoreParams = {
        objectKey,
        uploadId,
        storageClass,
        metaHeaders,
        eventualStorageBucket: bucketName,
        headers: request.headers,
        // The ownerID should be the account canonicalID.
        ownerID: accountCanonicalID,
        ownerDisplayName: authInfo.getAccountDisplayName(),
        // If initiator is an IAM user, the initiatorID is the ARN.
        // Otherwise, it is the same as the ownerID (the account canonicalID)
        initiatorID,
        // If initiator is an IAM user, the initiatorDisplayName is the
        // IAM user's displayname.
        // Otherwise, it is the same as the ownerDisplayName.
        initiatorDisplayName,
        splitter: constants.splitter,
    };
    const xmlParams = {
        bucketName,
        objectKey,
        uploadId,
    };
    const xml = _convertToXml(xmlParams);

    function _storetheMPObject(destinationBucket) {
        const serverSideEncryption =
            destinationBucket.getServerSideEncryption();
        let cipherBundle = null;
        if (serverSideEncryption) {
            cipherBundle = {
                algorithm: serverSideEncryption.algorithm,
                masterKeyId: serverSideEncryption.masterKeyId,
            };
        }
        services.getMPUBucket(destinationBucket, bucketName, log,
            (err, MPUbucket) => {
                // BACKWARD: Remove to remove the old splitter
                if (MPUbucket.getMdBucketModelVersion() < 2) {
                    metadataStoreParams.splitter = constants.oldSplitter;
                }
                services.metadataStoreMPObject(MPUbucket.getName(),
                    cipherBundle, metadataStoreParams, log, err => {
                        if (err) {
                            return callback(err);
                        }
                        pushMetric('initiateMultipartUpload', log, {
                            bucket: bucketName,
                        });
                        return callback(err, xml);
                    });
            });
    }

    services.metadataValidateAuthorization(metadataValParams,
        (err, destinationBucket) => {
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'services.metadataValidateAuthorization',
                });
                return callback(err);
            }
            if (destinationBucket.hasDeletedFlag() &&
                accountCanonicalID !== destinationBucket.getOwner()) {
                log.trace('deleted flag on bucket and request ' +
                    'from non-owner account');
                return callback(errors.NoSuchBucket);
            }
            if (destinationBucket.hasTransientFlag() ||
                destinationBucket.hasDeletedFlag()) {
                log.trace('transient or deleted flag so cleaning up bucket');
                return cleanUpBucket(destinationBucket,
                        accountCanonicalID, log, err => {
                            if (err) {
                                log.debug('error cleaning up bucket with flag',
                                { error: err,
                                transientFlag:
                                    destinationBucket.hasTransientFlag(),
                                deletedFlag:
                                    destinationBucket.hasDeletedFlag(),
                                });
                                // To avoid confusing user with error
                                // from cleaning up
                                // bucket return InternalError
                                return callback(errors.InternalError);
                            }
                            return _storetheMPObject(destinationBucket);
                        });
            }
            return _storetheMPObject(destinationBucket);
        });
}
