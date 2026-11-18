const { promisify } = require('util');
const { errors, constants: arsenalConstants } = require('@scality/arsenal');

const { config } = require('../Config');
const recordStore = require('./apiUtils/notificationDestination/recordStore');
const parseDestination = require('./apiUtils/notificationDestination/parseDestination');
const { nameError, recordModelVersion, requesterAccountId } = require('./apiUtils/notificationDestination/request');
const parseXML = require('../utilities/parseXML');
const monitoring = require('../utilities/monitoringHandler');

const METRICS_ACTION = 'createBucketNotificationDestination';

const parseXMLAsync = promisify(parseXML);
const getDestination = promisify((...args) => recordStore.getDestination(...args));
const putDestination = promisify((...args) => recordStore.putDestination(...args));

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
 * @param {function} [callback] - callback(err), when the caller gives one
 * @return {Promise<undefined>} - resolves after the record store writes the record
 */
async function notificationDestinationCreate(authInfo, request, log, callback) {
    if (callback) {
        return notificationDestinationCreate(authInfo, request, log).then(
            () => callback(null),
            err => callback(err),
        );
    }

    log.debug('processing request', { method: 'notificationDestinationCreate' });
    const name = request.query.name;

    const invalidName = nameError(name);
    if (invalidName) {
        log.debug('invalid destination name', { method: 'notificationDestinationCreate' });
        monitoring.promMetrics('PUT', request.bucketName, 400, METRICS_ACTION);
        throw invalidName;
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationCreate', destinationName: name, accountId };

    let parsedXml;
    try {
        parsedXml = await parseXMLAsync(request.post, log);
    } catch (parseErr) {
        monitoring.promMetrics('PUT', request.bucketName, parseErr.code, METRICS_ACTION);
        throw parseErr;
    }
    const fields = parseDestination(parsedXml);
    if (fields.error) {
        log.debug('invalid destination', { ...logFields, error: fields.error.description });
        monitoring.promMetrics('PUT', request.bucketName, fields.error.code, METRICS_ACTION);
        throw fields.error;
    }
    if (isGlobalDestinationName(name)) {
        log.debug('destination name is used by a global destination', logFields);
        monitoring.promMetrics('PUT', request.bucketName, errors.DestinationAlreadyExists.code, METRICS_ACTION);
        throw errors.DestinationAlreadyExists;
    }
    // Read, then write. bucketd has no conditional put. Thus, when two
    // creates of one name both read before either writes, both succeed,
    // and the last write wins.
    let exists = true;
    try {
        await getDestination(accountId, name, log);
    } catch (getErr) {
        if (!getErr.is?.NoSuchDestination) {
            log.error('could not read the destination record', { ...logFields, error: getErr });
            monitoring.promMetrics('PUT', request.bucketName, getErr.code, METRICS_ACTION);
            throw getErr;
        }
        exists = false;
    }
    if (exists) {
        log.debug('destination already exists', logFields);
        monitoring.promMetrics('PUT', request.bucketName, errors.DestinationAlreadyExists.code, METRICS_ACTION);
        throw errors.DestinationAlreadyExists;
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
    try {
        await putDestination(accountId, name, record, log);
    } catch (putErr) {
        log.error('could not write the destination record', { ...logFields, error: putErr });
        monitoring.promMetrics('PUT', request.bucketName, putErr.code, METRICS_ACTION);
        throw putErr;
    }
    log.debug('destination created', logFields);
    monitoring.promMetrics('PUT', request.bucketName, '200', METRICS_ACTION);
    return undefined;
}

module.exports = notificationDestinationCreate;
