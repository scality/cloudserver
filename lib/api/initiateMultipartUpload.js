import async from 'async';
import UUID from 'node-uuid';
import xml from 'xml';

import utils from '../utils.js';
import services from '../services.js';
import config from '../../config';
const splitter = config.splitter;


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

function _constructJSON(xmlParams) {
    const date = new Date();
    let month = (date.getMonth() + 1).toString();
    if (month.length === 1) {
        month = `0${month}`;
    }

    const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;

    return {
        InitiateMultipartUploadResult: [
            {
                _attr: {
                    xmlns: `http://${xmlParams.hostname}/doc/${dateString}`
                }
            },
            {
                Bucket: [xmlParams.bucketname]
            },
            {
                Key: [xmlParams.objectKey]
            },
            {
                UploadId: [xmlParams.uploadId]
            }
        ]};
}

function _convertToXml(xmlParams) {
    const constructedJSON = _constructJSON(xmlParams);
    return xml(constructedJSON, { declaration: { encoding: 'UTF-8' }});
}

/**
 * Initiate multipart upload returning xml that includes the UploadId
 * for the multipart upload
 * @param  {string}   accessKey - user access key
 * @param  {metastore}   metastore - metadata storage endpoint
 * @param  {request}   request   - request object given by router,
 * includes normalized headers
 * @param  {function} callback  - final callback to call with the result
 * @return {function} calls callback from router
 * with err and result as arguments
 */
export default
function initiateMultipartUpload(accessKey, metastore, request, callback) {
    const bucketname = utils.getResourceNames(request).bucket;
    const objectKey = utils.getResourceNames(request).object;
    // Note that we are using the string set forth in config.splitter
    // to split components in the storage
    // of each MPU.  AWS does not restrict characters in object keys so
    // there is the possiblity that the chosen splitter will occur in the object
    // name itself. To prevent this, we are restricting the creation of a
    // multipart upload object with a key containing the splitter.
    // TODO: Create system to pull data from saved
    // string so that no object names are
    // restricted.  This is GH Issue#88
    if (objectKey.indexOf(splitter) > -1) {
        return callback('InvalidArgument');
    }
    const hostname = utils.getResourceNames(request).host;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    const uploadId = UUID.v4();
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
    // TODO: Add this as a utility function for all object put requests
    // but after authentication so that string to sign is not impacted
    // This is GH Issue#89
    const storageClassOptions =
        ['standard', 'standard_ia', 'reduced_redundancy'];
    if (storageClassOptions.indexOf(request
        .lowerCaseHeaders['x-amz-storage-class']) < 0) {
        request.lowerCaseHeaders['x-amz-storage-class'] = 'STANDARD';
    }
    const storageClass = request.lowerCaseHeaders['x-amz-storage-​class'];
    const metadataValParams = {
        accessKey,
        bucketUID,
        objectKey,
        metastore,
        // Required permissions for this action are same as objectPut
        requestType: 'objectPut',
    };
    const metadataStoreParams = {
        objectKey,
        accessKey,
        uploadId,
        storageClass,
        metaHeaders,
        eventualStorageBucket: bucketname,
        headers: request.lowerCaseHeaders,
        // The ownerID should be the account canonicalID.
        // The below assumes the accessKey is the account canonicalID.
        ownerID: accessKey,
        // TODO: Need to get displayName for account from Vault
        // when checkAuth so can pass
        // info through to here
        ownerDisplayName: 'placeholder display name for now',
        // If initiator is an IAM user, the initiatorID is the ARN.
        // Otherwise, it is the same as the ownerID (the account canonicalID)
        initiatorID: accessKey,
        // If initiator is an IAM user, the initiatorDisplayName is the
        // IAM user's displayname.
        // Otherwise, it is the same as the ownerDisplayName.
        // TODO: Call this info from Vault when checkAuth
        // so can pass info through to here.
        initiatorDisplayName: 'placeholder display name for now',
    };
    const xmlParams = {
        bucketname,
        objectKey,
        uploadId,
        hostname,
    };

    async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams, next);
        },
        function waterfall2(destinationBucket, extraArg, next) {
            services.getMPUBucket(destinationBucket,
                metastore, bucketUID, next);
        },
        function waterfall2(MPUbucket, next) {
            services.metadataStoreMPObject(MPUbucket,
                metadataStoreParams, next);
        }
    ], function watefallFinal(err) {
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
