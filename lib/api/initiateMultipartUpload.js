import utils from '../utils.js';
import services from '../services.js';
import async from 'async';
import UUID from 'node-uuid';
import xml from 'xml';


/*
Sample xml response:
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
        "InitiateMultipartUploadResult": [
            {
                _attr: {
                    "xmlns": `http:\/\/${xmlParams.hostname}/doc/${dateString}`
                }
            },
            {
                "Bucket": [xmlParams.bucketname]
            },
            {
                "Key": [xmlParams.objectKey]
            },
            {
                "UploadId": [xmlParams.uploadId]
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
    const hostname = utils.getResourceNames(request).host;
    const metaHeaders = utils.getMetaHeaders(request.lowerCaseHeaders);
    const uploadId = UUID.v4();
    const bucketUID = utils.getResourceUID(request.namespace, bucketname);
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
        metaHeaders,
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
        function waterfall2(bucket, extraArg, next) {
            services.metadataStoreMPObject(bucket, metadataStoreParams, next);
        }
    ], function watefallFinal(err) {
        const xml = _convertToXml(xmlParams);
        return callback(err, xml);
    });
}
