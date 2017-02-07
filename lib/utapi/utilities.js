import { auth } from 'arsenal';
import { UtapiClient } from 'utapi';
import commander from 'commander';
import http from 'http';
import https from 'https';
import { logger } from '../utilities/logger';
import _config from '../Config';
// setup utapi client
const utapi = new UtapiClient(_config.utapi);

function _listMetrics(host,
                      port,
                      metric,
                      metricType,
                      timeRange,
                      accessKey,
                      secretKey,
                      verbose,
                      ssl) {
    const metricRoutes = {
        buckets: '/buckets?Action=ListMetrics',
        accounts: '/accounts?Action=ListMetrics',
    };
    const options = {
        host,
        port,
        method: 'POST',
        path: metricRoutes[metric],
        headers: {
            'content-type': 'application/json',
            'cache-control': 'no-cache',
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
    request.path = '/buckets';
    auth.client.generateV4Headers(request, { Action: 'ListMetrics' },
        accessKey, secretKey, 's3');
    request.path = '/buckets?Action=ListMetrics';
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }
    const requestObj = { timeRange };
    requestObj[metric] = metricType;
    request.write(JSON.stringify(requestObj));
    request.end();
}

/**
 * This function is used as a binary to send a request to utapi server
 * to list metrics for buckets or accounts
 * @param {string} [metricType] - (optional) Defined as 'buckets' if old style
 * bucket metrics listing
 * @return {undefined}
 */
export function listMetrics(metricType) {
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
            .option('--accounts <accounts>', 'Name of account(s) with a ' +
                'comma separator if more than one');
    }
    commander
        .option('-s, --start <start>', 'Start of time range')
        .option('-e --end <end>', 'End of time range')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('--ssl', 'Enable ssl')
        .option('-v, --verbose')
        .parse(process.argv);

    const { host, port, accessKey, secretKey, start, end, verbose,
        ssl } =
        commander;
    const requiredOptions = { host, port, accessKey, secretKey };
    // If not old style bucket metrics, we require usage of the metric option
    if (metricType !== 'buckets') {
        requiredOptions.metric = commander.metric;
        // Only allow 'buckets' or 'accounts' as the metric
        if (commander.metric !== 'buckets' && commander.metric !== 'accounts') {
            logger.error('metric must be buckets or accounts');
            commander.outputHelp();
            process.exit(1);
            return;
        }
    }
    // If old style bucket metrics, `metricType` will be 'buckets'. Otherwise,
    // `commander.metric` should be defined.
    const metric = metricType === 'buckets' ? 'buckets' : commander.metric;
    requiredOptions[metric] = commander[metric];

    Object.keys(requiredOptions).forEach(option => {
        if (!requiredOptions[option]) {
            logger.error(`missing required option: ${option}`);
            commander.outputHelp();
            process.exit(1);
            return;
        }
    });

    const numStart = Number.parseInt(start, 10);
    if (!numStart) {
        logger.error('start must be a number');
        commander.outputHelp();
        process.exit(1);
    }

    const timeRange = [numStart];

    if (end) {
        const numEnd = Number.parseInt(end, 10);
        if (!numEnd) {
            logger.error('if provide end, end must be a number');
            commander.outputHelp();
            process.exit(1);
        }
        timeRange.push(numEnd);
    }
    // The string `commander[metric]` is a comma-separated list of buckets or
    // accounts given by the user.
    const resources = commander[metric].split(',');
    _listMetrics(host, port, metric, resources, timeRange, accessKey, secretKey,
        verbose, ssl);
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
 * @param {number} [metricObj.byteLength] - (optional) current object size
 * (used, for example, for pushing 'deleteObject' metrics)
 * @param {number} [metricObj.newByteLength] - (optional) new object size
 * @param {number|null} [metricObj.oldByteLength] - (optional) old object size
 * (obj. overwrites)
 * @param {number} [metricObj.numberOfObjects] - (optional) number of obects
 * added/deleted
 * @return {function} - `utapi.pushMetric`
 */
export function pushMetric(action, log, metricObj) {
    let accountId = undefined;
    // If `authInfo` is included by the API, get the account's canonical ID for
    // account-level metrics.
    if (metricObj.authInfo) {
        accountId = metricObj.authInfo.getCanonicalID();
    }
    const { bucket, byteLength, newByteLength, oldByteLength,
        numberOfObjects } = metricObj;
    return utapi.pushMetric(action, log.getSerializedUids(), {
        bucket,
        accountId,
        byteLength,
        newByteLength,
        oldByteLength,
        numberOfObjects,
    });
}
