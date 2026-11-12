const { errors, errorInstances, s3middleware } = require('@scality/arsenal');

const { listingHardLimit } = require('../../constants');
const recordStore = require('./apiUtils/notificationDestination/recordStore');
const { requesterAccountId } = require('./apiUtils/notificationDestination/request');
const { destinationXml } = require('./apiUtils/notificationDestination/xml');
const monitoring = require('../utilities/monitoringHandler');

const { escapeForXml } = s3middleware;

const METRICS_ACTION = 'listNotificationDestinations';
const defaultMaxItems = 100;

/**
 * Return the number of names that a page holds. It is 100 when max-items is
 * absent. Else it is max-items, which must be digits only and from 1 to the
 * listing limit, 1000.
 * @param {*} maxItems - the max-items query parameter
 * @return {number|null} - the number of names, or null when max-items is
 * not valid
 */
function pageSize(maxItems) {
    if (maxItems === undefined) {
        return defaultMaxItems;
    }
    // A repeated parameter is an array, which a pattern test would coerce.
    if (typeof maxItems !== 'string' || !/^[0-9]+$/.test(maxItems)) {
        return null;
    }
    const size = Number.parseInt(maxItems, 10);
    return size >= 1 && size <= listingHardLimit ? size : null;
}

/**
 * notificationDestinationList - list the bucket notification destinations of
 * the account of the requester, in name order, each with its Auth reduced to
 * its Type
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback(err, xml)
 * @return {undefined}
 */
function notificationDestinationList(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'notificationDestinationList' });
    const { query } = request;

    const maxKeys = pageSize(query['max-items']);
    if (maxKeys === null) {
        log.debug('invalid max-items', { method: 'notificationDestinationList' });
        const err = errorInstances.InvalidArgument.customizeDescription(
            `max-items must be a whole number from 1 to ${listingHardLimit}.`,
        );
        monitoring.promMetrics('GET', request.bucketName, err.code, METRICS_ACTION);
        return process.nextTick(callback, err, null);
    }
    const { marker } = query;
    if (marker !== undefined && typeof marker !== 'string') {
        log.debug('invalid marker', { method: 'notificationDestinationList' });
        const err = errorInstances.InvalidArgument.customizeDescription('marker must be given once.');
        monitoring.promMetrics('GET', request.bucketName, err.code, METRICS_ACTION);
        return process.nextTick(callback, err, null);
    }
    const accountId = requesterAccountId(authInfo);
    const logFields = { method: 'notificationDestinationList', accountId, maxItems: maxKeys };
    const params = { maxKeys };
    if (marker !== undefined) {
        params.marker = marker;
    }

    return recordStore.listDestinations(accountId, params, log, (err, result) => {
        if (err) {
            log.error('could not list the destination records', { ...logFields, error: err });
            monitoring.promMetrics('GET', request.bucketName, err.code, METRICS_ACTION);
            return callback(err, null);
        }
        const { destinations, isTruncated, nextMarker } = result;
        let xml;
        try {
            xml = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<ListNotificationDestinationsResult>',
                `<IsTruncated>${isTruncated}</IsTruncated>`,
                isTruncated && nextMarker !== undefined ? `<NextMarker>${escapeForXml(nextMarker)}</NextMarker>` : '',
                ...destinations.map(destination => destinationXml(destination.record)),
                '</ListNotificationDestinationsResult>',
            ].join('');
        } catch (xmlErr) {
            // The handler cannot answer a record that lacks a block. Never let
            // the error escape the record store callback.
            log.error('could not build the destination list answer', { ...logFields, error: xmlErr.message });
            monitoring.promMetrics('GET', request.bucketName, errors.InternalError.code, METRICS_ACTION);
            return callback(errors.InternalError, null);
        }
        log.debug('destinations listed', { ...logFields, count: destinations.length, isTruncated });
        monitoring.promMetrics('GET', request.bucketName, '200', METRICS_ACTION);
        return callback(null, xml);
    });
}

module.exports = notificationDestinationList;
