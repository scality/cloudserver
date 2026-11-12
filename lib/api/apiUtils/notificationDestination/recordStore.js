const { Backoff, errors, models, storage } = require('@scality/arsenal');
const bucketclient = require('@scality/bucketclient');

const { config } = require('../../../Config');
const constants = require('../../../../constants');
const logger = require('../../../utilities/logger');

const { BucketInfo } = models;
const { MetadataWrapper } = storage.metadata;

/**
 * The record store is the only code that reads or writes bucket notification
 * destination records. Each record lives in the records bucket under
 * <accountId><splitter>bn-dest<splitter><name>. The marker record, outside
 * every account prefix, holds the key format that this code writes.
 */
const recordsBucket = constants.accountResourcesBucket;
const resourceTag = constants.notificationDestinationKeyTag;
const markerKey = `format${constants.splitter}${resourceTag}`;
const keyFormat = 1;
const bucketOwner = 'admin';

// The marker retries wait 1s, then twice as long each time, up to 30s.
// After a bucketd outage, every worker of every instance retries at once.
// The jitter spreads each delay by up to 10%, so that these retries do not
// reach bucketd at the same time.
const retryBackoff = new Backoff({ min: 1000, max: 30000, factor: 2, jitter: 0.1 });

const states = Object.freeze({
    STARTING: 'starting',
    READY: 'ready',
    UNKNOWN_FORMAT: 'unknownFormat',
});

let client = null;
let currentState = states.STARTING;
let retryTimer = null;
// stop increments this counter. Thus a marker read that is in flight when
// the store stops does not change the state.
let generation = 0;

// The S3 listing parser keeps only S3 object fields: keep the whole record.
function customListingParser(entries) {
    return entries.map(entry => ({ key: entry.key, value: JSON.parse(entry.value) }));
}

/**
 * Return the parameters of the metadata client of the records. On scality,
 * the client uses the record cluster, never the S3 metadata cluster. On
 * file, the client uses the metadata server of S3, with the parameters of
 * lib/metadata/wrapper.js.
 * @param {object} cfg - the parsed config
 * @return {object} - the MetadataWrapper parameters
 */
function clientParams(cfg) {
    if (cfg.backends.metadata === 'file') {
        return {
            metadataClient: {
                host: cfg.metadataClient.host,
                port: cfg.metadataClient.port,
            },
            constants: {
                usersBucket: constants.usersBucket,
                splitter: constants.splitter,
            },
            noDbOpen: null,
            customListingParser,
        };
    }
    if (cfg.backends.metadata !== 'scality') {
        return { customListingParser };
    }
    return {
        bucketdBootstrap: cfg.notificationDestinationApi.bucketd.bootstrap,
        bucketdLog: cfg.bucketd?.log,
        https: cfg.https,
        customListingParser,
    };
}

/**
 * Return the metadata client of the records. This function builds the
 * client on first use. Thus nothing connects to the record cluster while
 * the flag is off.
 * @return {MetadataWrapper} - the client
 */
function getClient() {
    if (client === null) {
        client = new MetadataWrapper(config.backends.metadata, clientParams(config), bucketclient, logger);
    }
    return client;
}

function accountPrefix(accountId) {
    return `${accountId}${constants.splitter}${resourceTag}${constants.splitter}`;
}

function recordKey(accountId, name) {
    return `${accountPrefix(accountId)}${name}`;
}

// Put a value. When the records bucket does not exist yet, create it, as the
// users bucket is created on its first write.
function putWithBucket(key, value, log, cb) {
    const md = getClient();
    return md.putObjectMD(recordsBucket, key, value, {}, log, err => {
        if (!err?.is?.NoSuchBucket) {
            return cb(err);
        }
        log.trace('records bucket does not exist, creating it');
        const bucketMD = new BucketInfo(
            recordsBucket,
            bucketOwner,
            bucketOwner,
            new Date().toJSON(),
            BucketInfo.currentModelVersion(),
        );
        return md.createBucket(recordsBucket, bucketMD, log, createErr => {
            // Another request may have created the bucket in between.
            if (createErr && !createErr.is?.BucketAlreadyExists) {
                log.error('could not create the records bucket', { error: createErr });
                return cb(createErr);
            }
            return md.putObjectMD(recordsBucket, key, value, {}, log, err => cb(err));
        });
    });
}

/**
 * Get one destination record.
 * @param {string} accountId - the account that owns the destination
 * @param {string} name - the destination name
 * @param {object} log - the request logger
 * @param {function} cb - cb(err, record), NoSuchDestination when absent
 * @return {undefined}
 */
function getDestination(accountId, name, log, cb) {
    return getClient().getObjectMD(recordsBucket, recordKey(accountId, name), {}, log, (err, record) => {
        if (err?.is?.NoSuchKey || err?.is?.NoSuchBucket) {
            return cb(errors.NoSuchDestination);
        }
        if (err) {
            return cb(err);
        }
        return cb(null, record);
    });
}

/**
 * Write one destination record, replacing any record of that name.
 * @param {string} accountId - the account that owns the destination
 * @param {string} name - the destination name
 * @param {object} record - the record
 * @param {object} log - the request logger
 * @param {function} cb - cb(err)
 * @return {undefined}
 */
function putDestination(accountId, name, record, log, cb) {
    return putWithBucket(recordKey(accountId, name), record, log, cb);
}

/**
 * Delete one destination record.
 * @param {string} accountId - the account that owns the destination
 * @param {string} name - the destination name
 * @param {object} log - the request logger
 * @param {function} cb - cb(err), NoSuchDestination when absent. On the
 * file backend, the delete of a missing key calls back with no error.
 * @return {undefined}
 */
function deleteDestination(accountId, name, log, cb) {
    return getClient().deleteObjectMD(recordsBucket, recordKey(accountId, name), {}, log, err => {
        if (err?.is?.NoSuchKey || err?.is?.NoSuchBucket) {
            return cb(errors.NoSuchDestination);
        }
        return cb(err);
    });
}

/**
 * List the destination records of one account, in name order.
 * @param {string} accountId - the account that owns the destinations
 * @param {object} params - listing parameters
 * @param {string} [params.marker] - list the names after this one
 * @param {number} params.maxKeys - the most names to return
 * @param {object} log - the request logger
 * @param {function} cb - cb(err, { destinations: [{ name, record }],
 * isTruncated, nextMarker }), nextMarker set only when truncated
 * @return {undefined}
 */
function listDestinations(accountId, params, log, cb) {
    const prefix = accountPrefix(accountId);
    const listingParams = { prefix, maxKeys: params.maxKeys };
    if (params.marker !== undefined) {
        listingParams.marker = `${prefix}${params.marker}`;
    }
    return getClient().listObject(recordsBucket, listingParams, log, (err, res) => {
        if (err?.is?.NoSuchBucket) {
            return cb(null, { destinations: [], isTruncated: false });
        }
        if (err) {
            return cb(err);
        }
        const destinations = res.Contents.map(entry => ({
            name: entry.key.slice(prefix.length),
            record: entry.value,
        }));
        const result = { destinations, isTruncated: Boolean(res.IsTruncated) };
        // the listing gives a NextMarker only with a delimiter
        if (result.isTruncated && destinations.length > 0) {
            result.nextMarker = destinations[destinations.length - 1].name;
        }
        return cb(null, result);
    });
}

/**
 * Return the state of the record store. The state is starting until the
 * store reads the marker. It is ready when the marker holds the key format
 * of this code, and unknownFormat when the marker holds another format.
 * @return {string} - starting, ready or unknownFormat
 */
function state() {
    return currentState;
}

/**
 * Set the client up, then read the marker, and write it when it is missing.
 * After a transport error, the store stays starting and retries with a
 * back-off. The caller never waits.
 * @param {object} log - a logger
 * @param {function} [cb] - cb() once the store is ready or unknownFormat
 * @return {undefined}
 */
function start(log, cb) {
    const done = cb || (() => {});
    const startGeneration = generation;
    // True after a marker write. Thus, when the read after the write still
    // finds no marker, the store waits for the back-off. It does not write the
    // marker again at once.
    let written = false;

    function retry(error, step = attempt) {
        const delay = retryBackoff.duration();
        log.warn('could not settle the destination record marker, retrying', {
            error,
            retryInMs: delay,
        });
        retryTimer = setTimeout(step, delay);
        retryTimer.unref?.();
    }

    function settle(newState) {
        retryBackoff.reset();
        currentState = newState;
        done();
    }

    function attempt() {
        retryTimer = null;
        getClient().getObjectMD(recordsBucket, markerKey, {}, log, (err, marker) => {
            if (startGeneration !== generation) {
                return undefined;
            }
            if ((err?.is?.NoSuchKey || err?.is?.NoSuchBucket) && written) {
                written = false;
                return retry(err);
            }
            if (err?.is?.NoSuchKey || err?.is?.NoSuchBucket) {
                return putWithBucket(markerKey, { keyFormat }, log, putErr => {
                    if (startGeneration !== generation) {
                        return undefined;
                    }
                    if (putErr) {
                        return retry(putErr);
                    }
                    log.info('destination record marker written', { keyFormat });
                    // Read the marker again, in case another instance wrote
                    // another format in between. This narrows the race but
                    // does not close it. bucketd has no conditional put. Thus
                    // an instance that writes after this read wins, and this
                    // instance does not see that write.
                    written = true;
                    return attempt();
                });
            }
            if (err) {
                return retry(err);
            }
            if (marker?.keyFormat !== keyFormat) {
                log.error('destination record marker holds an unknown key format, destination API refused', {
                    keyFormat: marker?.keyFormat,
                    expected: keyFormat,
                });
                return settle(states.UNKNOWN_FORMAT);
            }
            return settle(states.READY);
        });
    }

    // The file client opens its database in setup. The other clients need
    // no setup and call back at once. A failed setup goes through the same
    // back-off as a failed marker read. The setup of the file client can
    // also throw later, in its own callback. This code cannot catch that
    // throw.
    function setUpClient() {
        retryTimer = null;
        let calledBack = false;
        const onSetUp = err => {
            calledBack = true;
            if (startGeneration !== generation) {
                return undefined;
            }
            if (err) {
                return retry(err, setUpClient);
            }
            return attempt();
        };
        try {
            getClient().setup(onSetUp);
        } catch (err) {
            // Catch only a throw of the setup itself. A throw after a
            // synchronous callback comes from the next step: pass it on, so
            // that the callback never runs twice.
            if (calledBack) {
                throw err;
            }
            onSetUp(err);
        }
    }

    setUpClient();
}

/**
 * Stop the marker retries, go back to starting, and drop the client. The
 * next call builds a new client from the config.
 * @return {undefined}
 */
function stop() {
    generation += 1;
    if (retryTimer !== null) {
        clearTimeout(retryTimer);
        retryTimer = null;
    }
    retryBackoff.reset();
    currentState = states.STARTING;
    client = null;
}

module.exports = {
    getDestination,
    putDestination,
    deleteDestination,
    listDestinations,
    state,
    start,
    stop,
    getClient,
    clientParams,
    states,
};
