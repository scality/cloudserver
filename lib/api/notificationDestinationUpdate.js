const { promisify } = require('util');
const { errorInstances } = require('@scality/arsenal');

const recordStore = require('./apiUtils/notificationDestination/recordStore');
const parseDestination = require('./apiUtils/notificationDestination/parseDestination');
const { nameError, recordModelVersion, requesterAccountId } = require('./apiUtils/notificationDestination/request');
const parseXML = require('../utilities/parseXML');
const monitoring = require('../utilities/monitoringHandler');

const METRICS_ACTION = 'updateBucketNotificationDestination';

const parseXMLAsync = promisify(parseXML);
const getDestination = promisify((...args) => recordStore.getDestination(...args));
const putDestination = promisify((...args) => recordStore.putDestination(...args));

/**
 * notificationDestinationUpdate - replace a bucket notification destination
 * of the account of the requester with the body, keeping its name, account,
 * ARN and creation time
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} [callback] - callback(err), when the caller gives one
 * @return {Promise<undefined>} - resolves after the record store writes the record
 */
async function notificationDestinationUpdate(authInfo, request, log, callback) {
    if (callback) {
        return notificationDestinationUpdate(authInfo, request, log).then(
            () => callback(null),
            err => callback(err),
        );
    }

    log.debug('processing request', { method: 'notificationDestinationUpdate' });
    const name = request.query.name;

    const invalidName = nameError(name);
    if (invalidName) {
        log.debug('invalid destination name', { method: 'notificationDestinationUpdate' });
        monitoring.promMetrics('POST', request.bucketName, 400, METRICS_ACTION);
        throw invalidName;
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationUpdate', destinationName: name, accountId };

    let parsedXml;
    try {
        parsedXml = await parseXMLAsync(request.post, log);
    } catch (parseErr) {
        monitoring.promMetrics('POST', request.bucketName, parseErr.code, METRICS_ACTION);
        throw parseErr;
    }
    const fields = parseDestination(parsedXml);
    if (fields.error) {
        log.debug('invalid destination', { ...logFields, error: fields.error.description });
        monitoring.promMetrics('POST', request.bucketName, fields.error.code, METRICS_ACTION);
        throw fields.error;
    }
    // Read, then write. bucketd has no conditional put. Thus, when an
    // update reads before a delete and writes after it, the update
    // answers 200 and restores the record. A newer server can also
    // write between this read and this write. The update then answers
    // 200 and drops the fields of that newer record.
    let stored;
    try {
        stored = await getDestination(accountId, name, log);
    } catch (getErr) {
        if (getErr.is?.NoSuchDestination) {
            log.debug('destination not found', logFields);
        } else {
            log.error('could not read the destination record', { ...logFields, error: getErr });
        }
        monitoring.promMetrics('POST', request.bucketName, getErr.code, METRICS_ACTION);
        throw getErr;
    }
    // A record of a newer version may hold fields that this server
    // would drop. Leave it to a server that knows that version. The
    // request is valid, and a retry can reach such a server, so the
    // answer is ServiceUnavailable.
    if (stored.modelVersion > recordModelVersion) {
        log.debug('destination record of a newer version', {
            ...logFields,
            modelVersion: stored.modelVersion,
        });
        const newerErr = errorInstances.ServiceUnavailable.customizeDescription(
            'This destination was written by a newer server (record version ' +
                `${stored.modelVersion}, this server knows ${recordModelVersion}). ` +
                'Update it from a server running the newer version.',
        );
        monitoring.promMetrics('POST', request.bucketName, newerErr.code, METRICS_ACTION);
        throw newerErr;
    }
    // Replace the whole record, in the version that this server
    // writes. Keep only the identity fields and the creation time
    // from the stored record.
    const record = {
        modelVersion: recordModelVersion,
        Name: stored.Name,
        AccountId: stored.AccountId,
        Arn: stored.Arn,
        Type: fields.Type,
        Connection: fields.Connection,
        Auth: fields.Auth,
        CreatedAt: stored.CreatedAt,
        UpdatedAt: new Date().toJSON(),
    };
    try {
        await putDestination(accountId, name, record, log);
    } catch (putErr) {
        log.error('could not write the destination record', { ...logFields, error: putErr });
        monitoring.promMetrics('POST', request.bucketName, putErr.code, METRICS_ACTION);
        throw putErr;
    }
    log.debug('destination updated', logFields);
    monitoring.promMetrics('POST', request.bucketName, '200', METRICS_ACTION);
    return undefined;
}

module.exports = notificationDestinationUpdate;
