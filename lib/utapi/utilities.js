const http = require('http');
const https = require('https');
const commander = require('commander');
const { auth } = require('arsenal');
const { UtapiClient, utapiVersion } = require('utapi');
const logger = require('../utilities/logger');
const _config = require('../Config').config;
const { suppressedUtapiEventFields: suppressedEventFields } = require('../../constants');
// setup utapi client
let utapiConfig;

if (utapiVersion === 1 && _config.utapi) {
    if (_config.utapi.redis === undefined) {
        utapiConfig = Object.assign({}, _config.utapi, { redis: _config.redis });
    } else {
        utapiConfig = Object.assign({}, _config.utapi);
    }
} else if (utapiVersion === 2) {
    utapiConfig = Object.assign({
        tls: _config.https,
        suppressedEventFields,
    }, _config.utapi || {});
}

const utapi = new UtapiClient(utapiConfig);

const bucketOwnerMetrics = [
    'completeMultipartUpload',
    'multiObjectDelete',
    'abortMultipartUpload',
    'copyObject',
    'deleteObject',
    'putObject',
    'uploadPartCopy',
    'uploadPart',
];

function evalAuthInfo(authInfo, canonicalID, action) {
    let accountId = authInfo.getCanonicalID();
    let userId = authInfo.isRequesterAnIAMUser() ?
        authInfo.getShortid() : undefined;
    // If action impacts 'numberOfObjectsStored' or 'storageUtilized' metric
    // only the bucket owner account's metrics should be updated
    const canonicalIdMatch = authInfo.getCanonicalID() === canonicalID;
    if (bucketOwnerMetrics.includes(action) && !canonicalIdMatch) {
        accountId = canonicalID;
        userId = undefined;
    }
    return {
        accountId,
        userId,
    };
}

function _listMetrics(host,
    port,
    metric,
    metricType,
    timeRange,
    accessKey,
    secretKey,
    verbose,
    recent,
    ssl) {
    const listAction = recent ? 'ListRecentMetrics' : 'ListMetrics';
    // If recent listing, we do not provide `timeRange` in the request
    const requestObj = recent
        ? { [metric]: metricType }
        : { timeRange, [metric]: metricType };
    const requestBody = JSON.stringify(requestObj);
    const options = {
        host,
        port,
        method: 'POST',
        path: `/${metric}?Action=${listAction}`,
        headers: {
            'content-type': 'application/json',
            'cache-control': 'no-cache',
            'content-length': Buffer.byteLength(requestBody),
        },
        rejectUnauthorized: false,
    };
    const transport = ssl ? https : http;
    const request = transport.request(options, response => {
        if (verbose) {
            logger.info('response status code', {
                statusCode: response.statusCode,
            });
            logger.info('response headers', { headers: response.headers });
        }
        const body = [];
        response.setEncoding('utf8');
        response.on('data', chunk => body.push(chunk));
        response.on('end', () => {
            const responseBody = JSON.parse(body.join(''));
            if (response.statusCode >= 200 && response.statusCode < 300) {
                process.stdout.write(JSON.stringify(responseBody, null, 2));
                process.stdout.write('\n');
                process.exit(0);
            } else {
                logger.error('request failed with HTTP Status ', {
                    statusCode: response.statusCode,
                    body: responseBody,
                });
                process.exit(1);
            }
        });
    });
    // TODO: cleanup with refactor of generateV4Headers
    request.path = `/${metric}`;
    auth.client.generateV4Headers(request, { Action: listAction },
        accessKey, secretKey, 's3');
    request.path = `/${metric}?Action=${listAction}`;
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }

    request.write(requestBody);
    request.end();
}

/**
 * This function is used as a binary to send a request to utapi server
 * to list metrics for buckets or accounts
 * @param {string} [metricType] - (optional) Defined as 'buckets' if old style
 * bucket metrics listing
 * @return {undefined}
 */
function listMetrics(metricType) {
    commander
        .version('0.0.1')
        .option('-a, --access-key <accessKey>', 'Access key id')
        .option('-k, --secret-key <secretKey>', 'Secret access key');
    // We want to continue support of previous bucket listing. Hence the ability
    // to specify `metricType`. Remove `if` statement and
    // bin/list_bucket_metrics.js when prior method of listing bucket metrics is
    // no longer supported.
    if (metricType === 'buckets') {
        commander
            .option('-b, --buckets <buckets>', 'Name of bucket(s) with ' +
                'a comma separator if more than one');
    } else {
        commander
            .option('-m, --metric <metric>', 'Metric type')
            .option('--buckets <buckets>', 'Name of bucket(s) with a comma ' +
                'separator if more than one')
            .option('--accounts <accounts>', 'Account ID(s) with a comma ' +
                'separator if more than one')
            .option('--users <users>', 'User ID(s) with a comma separator if ' +
                'more than one')
            .option('--service <service>', 'Name of service');
    }
    commander
        .option('-s, --start <start>', 'Start of time range')
        .option('-r, --recent', 'List metrics including the previous and ' +
            'current 15 minute interval')
        .option('-e --end <end>', 'End of time range')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('--ssl', 'Enable ssl')
        .option('-v, --verbose')
        .parse(process.argv);

    const { host, port, accessKey, secretKey, start, end, verbose, recent,
        ssl } =
        commander;
    const requiredOptions = { host, port, accessKey, secretKey };
    // If not old style bucket metrics, we require usage of the metric option
    if (metricType !== 'buckets') {
        requiredOptions.metric = commander.metric;
        const validMetrics = ['buckets', 'accounts', 'users', 'service'];
        if (validMetrics.indexOf(commander.metric) < 0) {
            logger.error('metric must be \'buckets\', \'accounts\', ' +
                '\'users\', or \'service\'');
            commander.outputHelp();
            process.exit(1);
            return;
        }
    }
    // If old style bucket metrics, `metricType` will be 'buckets'. Otherwise,
    // `commander.metric` should be defined.
    const metric = metricType === 'buckets' ? 'buckets' : commander.metric;
    requiredOptions[metric] = commander[metric];
    // If not recent listing, the start option must be provided
    if (!recent) {
        requiredOptions.start = commander.start;
    }
    Object.keys(requiredOptions).forEach(option => {
        if (!requiredOptions[option]) {
            logger.error(`missing required option: ${option}`);
            commander.outputHelp();
            process.exit(1);
        }
    });

    // The string `commander[metric]` is a comma-separated list of resources
    // given by the user.
    const resources = commander[metric].split(',');

    // Validate passed accounts to remove any canonicalIDs
    if (metric === 'accounts') {
        const invalid = resources.filter(r => !/^\d{12}$/.test(r));
        if (invalid.length > 0) {
            logger.error(`Invalid account ID: ${invalid.join(', ')}`);
            process.exit(1);
        }
    }

    const timeRange = [];
    // If recent listing, we disregard any start or end option given
    if (!recent) {
        const numStart = Number.parseInt(start, 10);
        if (!numStart) {
            logger.error('start must be a number');
            commander.outputHelp();
            process.exit(1);
            return;
        }
        timeRange.push(numStart);
        if (end) {
            const numEnd = Number.parseInt(end, 10);
            if (!numEnd) {
                logger.error('end must be a number');
                commander.outputHelp();
                process.exit(1);
                return;
            }
            timeRange.push(numEnd);
        }
    }

    _listMetrics(host, port, metric, resources, timeRange, accessKey, secretKey,
        verbose, recent, ssl);
}

/**
 * Call the Utapi Client `pushMetric` method with the associated parameters
 * @param {string} action - the metric action to push a metric for
 * @param {object} log - werelogs logger
 * @param {object} metricObj - the object containing the relevant data for
 * pushing metrics in Utapi
 * @param {string} [metricObj.bucket] - (optional) bucket name
 * @param {AuthInfo} [metricObj.authInfo] - (optional) Instance of AuthInfo
 * class with requester's info
 * @param {number} [metricObj.canonicalID] - (optional) The account's canonical
 * ID used for the request
 * @param {number} [metricObj.byteLength] - (optional) current object size
 * (used, for example, for pushing 'deleteObject' metrics)
 * @param {number} [metricObj.newByteLength] - (optional) new object size
 * @param {number|null} [metricObj.oldByteLength] - (optional) old object size
 * (obj. overwrites)
 * @param {number} [metricObj.numberOfObjects] - (optional) number of objects
 * added/deleted
 * @param {boolean} [metricObject.isDelete] - (optional) Indicates whether this
 * is a delete operation
 * @return {function} - `utapi.pushMetric`
 */
function pushMetric(action, log, metricObj) {
    const {
        bucket,
        keys,
        versionId,
        byteLength,
        newByteLength,
        oldByteLength,
        numberOfObjects,
        authInfo,
        canonicalID,
        location,
        isDelete,
        removedDeleteMarkers,
    } = metricObj;

    if (utapiVersion === 2) {
        const incomingBytes = action === 'getObject' ? 0 : newByteLength;
        let sizeDelta = incomingBytes;
        if (Number.isInteger(oldByteLength) && Number.isInteger(newByteLength)) {
            sizeDelta = newByteLength - oldByteLength;
            // Include oldByteLength in conditional so we don't end up with `-0`
        } else if (action === 'completeMultipartUpload' && !versionId && oldByteLength) {
            // If this is a non-versioned bucket we need to decrement
            // the sizeDelta added by uploadPart when completeMPU is called.
            sizeDelta = -oldByteLength;
        } else if (action === 'abortMultipartUpload' && byteLength) {
            sizeDelta = -byteLength;
        }

        let objectDelta = isDelete ? -numberOfObjects : numberOfObjects;
        // putDeleteMarkerObject does not pass numberOfObjects
        if (action === 'putDeleteMarkerObject'
            || action === 'replicateDelete'
            || action === 'replicateObject') {
            objectDelta = 1;
        } else if (action === 'multiObjectDelete') {
            objectDelta = -(numberOfObjects + removedDeleteMarkers);
        }

        const utapiObj = {
            operationId: action,
            bucket,
            location,
            objectDelta,
            sizeDelta: isDelete ? -byteLength : sizeDelta,
            incomingBytes,
            outgoingBytes: action === 'getObject' ? newByteLength : 0,
        };
        if (keys && keys.length === 1) {
            [utapiObj.object] = keys;
            if (versionId) {
                utapiObj.versionId = versionId;
            }
        }
        utapiObj.account = authInfo ? evalAuthInfo(authInfo, canonicalID, action).accountId : canonicalID;
        utapiObj.user = authInfo ? evalAuthInfo(authInfo, canonicalID, action).userId : '';
        return utapi.pushMetric(utapiObj);
    }
    const utapiObj = {
        bucket,
        keys,
        byteLength,
        newByteLength,
        oldByteLength,
        numberOfObjects,
    };
    // If `authInfo` is included by the API, get the account's canonical ID for
    // account-level metrics and the shortId for user-level metrics. Otherwise
    // check if the canonical ID is already provided for account-level metrics.
    if (authInfo) {
        const { accountId, userId } = evalAuthInfo(authInfo, canonicalID, action);
        utapiObj.accountId = accountId;
        utapiObj.userId = userId;
    } else if (canonicalID) {
        utapiObj.accountId = canonicalID;
    }
    return utapi.pushMetric(action, log.getSerializedUids(), utapiObj);
}

/**
 * internal: get the unique location ID from the location name
 *
 * @param {string} location - location name
 * @return {string} - location unique ID
 */
function _getLocationId(location) {
    return _config.locationConstraints[location].objectId;
}

/**
 * Call the Utapi Client 'getLocationMetric' method with the
 * associated parameters
 * @param {string} location - name of data backend to list metric for
 * @param {object} log - werelogs logger
 * @param {function} cb - callback to call
 * @return {function} - `utapi.getLocationMetric`
 */
function getLocationMetric(location, log, cb) {
    const locationId = _getLocationId(location);
    return utapi.getLocationMetric(locationId, log.getSerializedUids(), cb);
}

/**
 * Call the Utapi Client 'pushLocationMetric' method with the
 * associated parameters
 * @param {string} location - name of data backend
 * @param {number} byteLength - number of bytes
 * @param {object} log - werelogs logger
 * @param {function} cb - callback to call
 * @return {function} - `utapi.pushLocationMetric`
 */
function pushLocationMetric(location, byteLength, log, cb) {
    const locationId = _getLocationId(location);
    return utapi.pushLocationMetric(locationId, byteLength,
        log.getSerializedUids(), cb);
}

module.exports = {
    listMetrics,
    pushMetric,
    getLocationMetric,
    pushLocationMetric,
};
