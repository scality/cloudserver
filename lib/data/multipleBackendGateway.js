import { errors } from 'arsenal';
import { Logger } from 'werelogs';
import async from 'async';

import config from '../Config';
import parseLC from './locationConstraintParser';

const logger = new Logger('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

const clients = parseLC(config);
let multBackendResp = {};

function _createAwsKey(requestBucketName, requestObjectKey,
    partNumber, uploadId, bucketMatch) {
    if (bucketMatch) {
        return `uploadId-${uploadId}/partNumber-${partNumber}/` +
            `${requestObjectKey}`;
    }
    return `${requestBucketName}/uploadId-${uploadId}/` +
        `partNumber-${partNumber}/${requestObjectKey}`;
}

function checkAWSHealth(client, location, callback) {
    client.headBucket({ Bucket: client.awsBucketName },
    err => {
        if (err) {
            multBackendResp[location] = { error: err };
            callback(null, multBackendResp);
        } else {
            client.getBucketVersioning({
                Bucket: client.awsBucketName },
            (err, data) => {
                if (err) {
                    multBackendResp[location] = { error: err };
                } else if (!data.Status ||
                data.Status === 'Suspended') {
                    multBackendResp[location] = {
                        versioningStatus: data.Status,
                        error: 'Versioning must be enabled',
                    };
                } else {
                    multBackendResp[location] = {
                        versioningStatus: data.Status,
                        message: 'Congrats! You own the bucket',
                    };
                }
                callback(null, multBackendResp);
            });
        }
    });
}

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
               keyContext.objectKey, partNumber, uploadId, client.bucketMatch);
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
                       const log = createLogger(reqUids);
                       log.error('err from data backend',
                       { err, dataStoreName: client.dataStoreName });
                       // TODO: consider passing through error
                       // rather than translating though could be confusing
                       // (e.g., NoSuchBucket error when request was
                       // actually made to the Scality s3 bucket name)
                       return callback(errors.InternalError);
                   }
                   const dataRetrievalInfo = {
                       key: awsKey,
                       dataStoreName: client.dataStoreName,
                       // because of encryption the ETag here could be
                       // different from our metadata so let's store it
                       dataStoreETag: data.ETag,
                   };
                   return callback(null, dataRetrievalInfo);
               });
        }
        return client.put(stream, size, keyContext,
            reqUids, (err, key) => {
                if (err) {
                    const log = createLogger(reqUids);
                    log.error('error from datastore',
                             { error: err, implName: client.clientType });
                    return callback(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: controllingLocationConstraint,
                };
                return callback(null, dataRetrievalInfo);
            });
    },

    get: (objectGetInfo, range, reqUids, callback) => {
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        const client = clients[objectGetInfo.dataStoreName];
        if (client.clientType === 'scality') {
            return client.get(objectGetInfo.key, range, reqUids, callback);
        }
        if (client.clientType === 'aws_s3') {
            const stream = client.getObject({
                Bucket: client.awsBucketName,
                Key: key,
                Range: range,
            }).createReadStream().on('error', err => {
                const log = createLogger(reqUids);
                log.error('error creating stream', { err });
                return callback(err);
            });
            return callback(null, stream);
        }
        return client.get(objectGetInfo, range, reqUids, callback);
    },

    delete: (objectGetInfo, reqUids, callback) => {
        const client = clients[objectGetInfo.dataStoreName];
        if (client.clientType === 'aws_s3') {
            return callback(null, client.deleteObject({
                Bucket: client.awsBucketName,
                Key: objectGetInfo.key,
            }));
        }
        if (client.clientType === 'scality') {
            return client.delete(objectGetInfo.key, reqUids, callback);
        }
        return client.delete(objectGetInfo, reqUids, callback);
    },

    healthcheck: (log, callback, deep) => {
        multBackendResp = {};
        const awsArray = [];
        async.each(Object.keys(clients), (location, cb) => {
            const client = clients[location];
            if (client.clientType === 'scality') {
                client.healthcheck(log, (err, res) => {
                    if (err) {
                        multBackendResp[location] = { error: err };
                    } else {
                        multBackendResp[location] = { code: res.statusCode,
                            message: res.statusMessage };
                    }
                    cb();
                });
            } else if (client.clientType === 'aws_s3') {
                if (deep) {
                    awsArray.push(location);
                    cb();
                } else {
                    checkAWSHealth(client, location, cb);
                }
            } else {
                // if backend type isn't 'scality' or 'aws_s3', it will be
                //  'mem' or 'file', for which the default response is 200 OK
                multBackendResp[location] = { code: 200, message: 'OK' };
                cb();
            }
        }, () => {
            if (awsArray.length > 0) {
                const randomAWS = awsArray[Math.floor(Math.random() *
                    awsArray.length)];
                const checkThisOne = clients[randomAWS];
                checkAWSHealth(checkThisOne, randomAWS, callback);
            } else {
                callback(null, multBackendResp);
            }
        });
    },
};

export default multipleBackendGateway;
