const recordStore = require('./apiUtils/notificationDestination/recordStore');
const { nameError, requesterAccountId } = require('./apiUtils/notificationDestination/request');
const monitoring = require('../utilities/monitoringHandler');

const METRICS_ACTION = 'deleteNotificationDestination';

/**
 * notificationDestinationDelete - delete a bucket notification destination
 * of the account of the requester
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback(err)
 * @return {undefined}
 */
function notificationDestinationDelete(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'notificationDestinationDelete' });
    const name = request.query.name;

    const invalidName = nameError(name);
    if (invalidName) {
        log.debug('invalid destination name', { method: 'notificationDestinationDelete' });
        monitoring.promMetrics('DELETE', request.bucketName, 400, METRICS_ACTION);
        return process.nextTick(callback, invalidName);
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationDelete', destinationName: name, accountId };

    return recordStore.getDestination(accountId, name, log, getErr => {
        if (getErr) {
            if (getErr.is?.NoSuchDestination) {
                log.debug('destination not found', logFields);
            } else {
                log.error('could not read the destination record', { ...logFields, error: getErr });
            }
            monitoring.promMetrics('DELETE', request.bucketName, getErr.code, METRICS_ACTION);
            return callback(getErr);
        }
        // Read, then delete. Another request can remove the record between
        // the two calls. On mem and bucketd, the backend reports the missing
        // key, so the answer is NoSuchDestination. On file, the delete of a
        // missing key succeeds, so the answer is 204. The end state is the
        // same.
        return recordStore.deleteDestination(accountId, name, log, deleteErr => {
            if (deleteErr) {
                if (deleteErr.is?.NoSuchDestination) {
                    log.debug('destination not found', logFields);
                } else {
                    log.error('could not delete the destination record', { ...logFields, error: deleteErr });
                }
                monitoring.promMetrics('DELETE', request.bucketName, deleteErr.code, METRICS_ACTION);
                return callback(deleteErr);
            }
            log.debug('destination deleted', logFields);
            monitoring.promMetrics('DELETE', request.bucketName, '204', METRICS_ACTION);
            return callback(null);
        });
    });
}

module.exports = notificationDestinationDelete;
