const { promisify } = require('util');
const { errors } = require('@scality/arsenal');

const recordStore = require('./apiUtils/notificationDestination/recordStore');
const { nameError, requesterAccountId } = require('./apiUtils/notificationDestination/request');
const { destinationXml } = require('./apiUtils/notificationDestination/xml');
const monitoring = require('../utilities/monitoringHandler');

const METRICS_ACTION = 'getBucketNotificationDestination';

const getDestination = promisify((...args) => recordStore.getDestination(...args));

/**
 * notificationDestinationGet - get a bucket notification destination of the
 * account of the requester, with its Auth reduced to its Type
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} [callback] - callback(err, xml), when the caller gives one
 * @return {Promise<string>} - the XML answer
 */
async function notificationDestinationGet(authInfo, request, log, callback) {
    if (callback) {
        return notificationDestinationGet(authInfo, request, log).then(
            xml => callback(null, xml),
            err => callback(err, null),
        );
    }

    log.debug('processing request', { method: 'notificationDestinationGet' });
    const name = request.query.name;

    const invalidName = nameError(name);
    if (invalidName) {
        log.debug('invalid destination name', { method: 'notificationDestinationGet' });
        monitoring.promMetrics('GET', request.bucketName, 400, METRICS_ACTION);
        throw invalidName;
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationGet', destinationName: name, accountId };

    let record;
    try {
        record = await getDestination(accountId, name, log);
    } catch (err) {
        if (err.is?.NoSuchDestination) {
            log.debug('destination not found', logFields);
        } else {
            log.error('could not read the destination record', { ...logFields, error: err });
        }
        monitoring.promMetrics('GET', request.bucketName, err.code, METRICS_ACTION);
        throw err;
    }
    let xml;
    try {
        xml = ['<?xml version="1.0" encoding="UTF-8"?>', destinationXml(record)].join('');
    } catch (xmlErr) {
        // The handler cannot answer a record that lacks a value. Answer
        // InternalError, not the error of the XML builder.
        log.error('could not build the destination answer', { ...logFields, error: xmlErr.message });
        monitoring.promMetrics('GET', request.bucketName, errors.InternalError.code, METRICS_ACTION);
        throw errors.InternalError;
    }
    log.debug('destination read', logFields);
    monitoring.promMetrics('GET', request.bucketName, '200', METRICS_ACTION);
    return xml;
}

module.exports = notificationDestinationGet;
