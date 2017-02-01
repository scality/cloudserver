import AWS from 'aws-sdk';
import UUID from 'node-uuid';
import Sproxy from 'sproxydclient';
import { errors } from 'arsenal';
import { Logger } from 'werelogs';

import file from './file/backend';
import inMemory from './in_memory/backend';
import config from '../Config';

const logger = new Logger('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

function _createAwsKey(requestBucketName, requestObjectKey,
    partNumber, uploadId) {
    const unique = UUID.v4();
    // TODO: Discuss how we want to generate keys. Not having unique feature
    // is too dangerous since we could have cleanup deletes on a key being
    // called after a new object was created
    return `${requestBucketName}/uploadId-${uploadId}/` +
        `partNumber-${partNumber}/${requestObjectKey}/${unique}`;
}

const clients = {};
Object.keys(config.locationConstraints).forEach(location => {
    const locationObj = config.locationConstraints[location];
    if (locationObj.type === 'mem') {
        clients[location] = inMemory;
    }
    if (locationObj.type === 'file') {
        clients[location] = file;
    }
    if (locationObj.type === 'scality_s3'
    && locationObj.information.connector === 'sproxyd') {
        clients[location] = new Sproxy({
            bootstrap: locationObj.information.connector
                .sproxyd.bootstrap,
            log: config.log,
            // Might be undefined which is ok since there is a default
            // set in sproxydclient if chordCos is undefined
            chordCos: locationObj.information.connector.sproxyd.chordCos,
        });
    }
    if (locationObj.type === 'aws_s3') {
        clients[location] = new AWS.S3({
            endpoint: `https://${locationObj.information.endpoint}`,
            // Non-file stream objects are not supported with SigV4 (node sdk)
            // so note that we are using the default of signatureVersion v2

            // consider disabling
            debug: true,
            // perhaps use this rather than setting ourselves. Not implemented yet for streams in node sdk!!!
            computeChecksums: true,
            credentials: new AWS.SharedIniFileCredentials({ profile:
                locationObj.information.credentialsProfile }),
        });
        clients[location].clientType = 'aws_s3';
        clients[location].awsBucketName = locationObj.information.bucketName;
        clients[location].dataStoreName = location;
    }
    if (locationObj.type === 'virtual-user-metadata') {
        // TODO
        // clients[location] = some sort of bucketclient
    }
});

const multipleBackendGateway = {
    put: (stream, size, keyContext, backendInfo, reqUids, callback) => {
        const controllingLocationConstraint =
            backendInfo.getControllingLocationConstraint();
        const client = clients[controllingLocationConstraint];
        if (!client) {
            const log = createLogger(reqUids);
            log.error('no data backend matching controlling locationConstraint',
            { controllingLocationConstraint });
            return process.nextTick(() => {
                callback(errors.InternalError);
            });
        }
        // client is AWS SDK
        if (client.clientType === 'aws_s3') {
            const partNumber = keyContext.partNumber || '00000';
            const uploadId = keyContext.uploadId || '00000';
            const awsKey = _createAwsKey(keyContext.bucketName,
                keyContext.objectKey, partNumber, uploadId);
            console.log("client.awsBucketName!!", client.awsBucketName)
            console.log("stream.completedHash!!", stream.completedHash)
            return client.putObject({
                Bucket: client.awsBucketName,
                Key: awsKey,
                Body: stream,
                ContentLength: size,
                //Must fix!!!  Use this or see if computeChecksums handles it
                //for us
                // TODO: This should be in listener to make sure
                // we have the completedHash. Also, if we pre-encrypt,
                // this will not work. Need to get hash of encrypted version.
                // Sending ContentMD5 is needed so that AWS will check to
                // make sure it is receiving the correct data.
                // ContentMD5: stream.completedHash,
            },
                (err, data) => {
                    if (err) {
                        console.log('error from aws!!', err);
                        const log = createLogger(reqUids);
                        log.error('err from data backend',
                        { err, dataStoreName: client.dataStoreName });
                        // TODO: consider passing through error
                        // rather than translating though could be confusing
                        // (e.g., NoSuchBucket error when request was
                        // actually made to the Scality s3 bucket name)
                        return callback(errors.InternalError);
                    }
                    console.log("data from aws!!", data);
                    const dataRetrievalInfo = {
                        key: awsKey,
                        dataStoreName: client.dataStoreName,
                        // because of encryption the ETag here could be
                        // different from our metadata so let's store it
                        dataStoreETag: data.ETag,
                    };
                    console.log("dataRetrievalInfo!!", dataRetrievalInfo);
                    return callback(null, dataRetrievalInfo);
                });
        }
        return client.put(stream, size, keyContext,
            reqUids, callback);
    },

    get: (objectGetInfo, range, reqUids, callback) => {
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        const client = clients[objectGetInfo.dataStoreName];
        console.log('client!!!!! ', client);
        console.log('\t OBJECTGETINFO: ', objectGetInfo);
        if (client.clientType === 'aws_s3') {
            return callback(null, client.getObject({
                Bucket: client.awsBucketName,
                Key: key,
                Range: range,
            }).createReadStream());
        }
        return client.get(objectGetInfo, range, reqUids, callback);
    },

    delete: (objectGetInfo, reqUids, callback) => {
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        const client = clients[objectGetInfo.dataStoreName];
        if (client.clientType === 'aws-s3') {
            return client.deleteObject({
                Bucket: client.awsBucketName,
                Key: key,
            });
        }
        return client.delete(objectGetInfo, reqUids, callback);
    },

    // checkHealth: to aggregate from multiple?
};

export default multipleBackendGateway;

// DO WRAPPER STUFF BASED ON REQUEST/STORED INFO
//
// For GETS and DELETES use objectGetInfo implName

// For PUTS:
// 1) check x-amz-meta-scal-location-constraint on put/copy
// 2) bucket location constraint
// 3) default for endpoint hit.
