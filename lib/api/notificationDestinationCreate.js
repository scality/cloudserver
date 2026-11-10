const { errors, constants: arsenalConstants } = require('@scality/arsenal');

const { config } = require('../Config');
const recordStore = require('./apiUtils/notificationDestination/recordStore');
const parseDestination = require('./apiUtils/notificationDestination/parseDestination');
const { nameError, recordModelVersion, requesterAccountId } = require('./apiUtils/notificationDestination/request');
const parseXML = require('../utilities/parseXML');
const monitoring = require('../utilities/monitoringHandler');

const METRICS_ACTION = 'createNotificationDestination';

// Create refuses a name that a global destination uses, because the legacy
// matchers of global destinations ignore the account.
function isGlobalDestinationName(name) {
    return (config.bucketNotificationDestinations || []).some(destination => destination.resource === name);
}

/**
 * notificationDestinationCreate - create a bucket notification destination
 * in the account of the requester
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback(err)
 * @return {undefined}
 */
function notificationDestinationCreate(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'notificationDestinationCreate' });
    const name = request.query.name;

    const invalidName = nameError(name);
    if (invalidName) {
        log.debug('invalid destination name', { method: 'notificationDestinationCreate' });
        monitoring.promMetrics('PUT', request.bucketName, 400, METRICS_ACTION);
        return process.nextTick(callback, invalidName);
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationCreate', destinationName: name, accountId };

    return parseXML(request.post, log, (parseErr, parsedXml) => {
        if (parseErr) {
            monitoring.promMetrics('PUT', request.bucketName, parseErr.code, METRICS_ACTION);
            return callback(parseErr);
        }
        const fields = parseDestination(parsedXml);
        if (fields.error) {
            log.debug('invalid destination', { ...logFields, error: fields.error.description });
            monitoring.promMetrics('PUT', request.bucketName, fields.error.code, METRICS_ACTION);
            return callback(fields.error);
        }
        if (isGlobalDestinationName(name)) {
            log.debug('destination name is used by a global destination', logFields);
            monitoring.promMetrics('PUT', request.bucketName, errors.DestinationAlreadyExists.code, METRICS_ACTION);
            return callback(errors.DestinationAlreadyExists);
        }
        // Read, then write. bucketd has no conditional put. Thus, when two
        // creates of one name both read before either writes, both succeed,
        // and the last write wins.
        return recordStore.getDestination(accountId, name, log, getErr => {
            if (!getErr) {
                log.debug('destination already exists', logFields);
                monitoring.promMetrics('PUT', request.bucketName, errors.DestinationAlreadyExists.code, METRICS_ACTION);
                return callback(errors.DestinationAlreadyExists);
            }
            if (!getErr.is?.NoSuchDestination) {
                log.error('could not read the destination record', { ...logFields, error: getErr });
                monitoring.promMetrics('PUT', request.bucketName, getErr.code, METRICS_ACTION);
                return callback(getErr);
            }
            const timestamp = new Date().toJSON();
            const record = {
                modelVersion: recordModelVersion,
                Name: name,
                AccountId: accountId,
                Arn: `${arsenalConstants.notificationArnPrefix}::${accountId}:${name}`,
                Type: fields.Type,
                Connection: fields.Connection,
                Auth: fields.Auth,
                CreatedAt: timestamp,
                UpdatedAt: timestamp,
            };
            return recordStore.putDestination(accountId, name, record, log, putErr => {
                if (putErr) {
                    log.error('could not write the destination record', { ...logFields, error: putErr });
                    monitoring.promMetrics('PUT', request.bucketName, putErr.code, METRICS_ACTION);
                    return callback(putErr);
                }
                log.debug('destination created', logFields);
                monitoring.promMetrics('PUT', request.bucketName, '200', METRICS_ACTION);
                return callback(null);
            });
        });
    });
}

module.exports = notificationDestinationCreate;
