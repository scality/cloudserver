import { errors } from 'arsenal';
import async from 'async';
import UUID from 'node-uuid';
import xml from 'xml';

import constants from '../../constants';
import services from '../services';
import utils from '../utils';

const splitter = constants.splitter;

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

 /*
    Construct JSON in proper format to be converted to XML
    to be returned to client
 */
function _constructJSON(xmlParams) {
    return {
        InitiateMultipartUploadResult: [
            { _attr: { xmlns: `http://s3.amazonaws.com/doc/2006-03-01/` } },
            { Bucket: [xmlParams.bucketName] },
            { Key: [xmlParams.objectKey] },
            { UploadId: [xmlParams.uploadId] },
        ] };
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' } });
}

/**
 * Initiate multipart upload returning xml that includes the UploadId
 * for the multipart upload
 * @param  {AuthInfo} Instance of AuthInfo class with requester's info
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} callback  - final callback to call with the result
 * @return {function} calls callback from router
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
    // TODO: Create system to pull data from saved
    // string so that no object names are
    // restricted.  This is GH Issue#88
    if (objectKey.indexOf(splitter) > -1) {
        return callback(errors.InvalidArgument);
    }
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
    let initiatorID = authInfo.getCanonicalID();
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
        ownerID: authInfo.getCanonicalID(),
        ownerDisplayName: authInfo.getAccountDisplayName(),
        // If initiator is an IAM user, the initiatorID is the ARN.
        // Otherwise, it is the same as the ownerID (the account canonicalID)
        initiatorID,
        // If initiator is an IAM user, the initiatorDisplayName is the
        // IAM user's displayname.
        // Otherwise, it is the same as the ownerDisplayName.
        initiatorDisplayName,
    };
    const xmlParams = {
        bucketName,
        objectKey,
        uploadId,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams,
                (err, destinationBucket) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null, destinationBucket);
                });
        },
        function waterfall2(destinationBucket, next) {
            services.getMPUBucket(destinationBucket, bucketName, log, next);
        },
        function waterfall2(MPUbucket, next) {
            services.metadataStoreMPObject(MPUbucket.name, metadataStoreParams,
                log, next);
        },
    ], function watefallFinal(err) {
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
