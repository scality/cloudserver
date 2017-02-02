import { auth } from 'arsenal';
import commander from 'commander';
import http from 'http';
import https from 'https';
import { logger } from '../utilities/logger';

function _listBucketMetrics(host,
                            port,
                            buckets,
                            timeRange,
                            accessKey,
                            secretKey,
                            verbose,
                            ssl) {
    const options = {
        host,
        port,
        method: 'POST',
        path: '/buckets?Action=ListMetrics',
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
                // eslint-disable-next-line no-console
                console.log(responseBody);
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
    request.write(JSON.stringify({ buckets, timeRange }));
    request.end();
}

/**
 * This function is used as a binary to send a request to utapi server
 * to list bucket metrics
 *
 * @return {undefined}
 */
export function listBucketMetrics() {
    commander
        .version('0.0.1')
        .option('-a, --access-key <accessKey>', 'Access key id')
        .option('-k, --secret-key <secretKey>', 'Secret access key')
        .option('-b, --buckets <buckets>', 'Name of bucket(s)' +
        'with a comma separator if more than one')
        .option('-s, --start <start>', 'Start of time range')
        .option('-e --end <end>', 'End of time range')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('--ssl', 'Enable ssl')
        .option('-v, --verbose')
        .parse(process.argv);

    const { host, port, accessKey, secretKey, start, end,
        buckets, verbose, ssl } =
        commander;
    const requiredOptions = { host, port, accessKey, secretKey, buckets,
        start };
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

    const bucketArr = buckets.split(',');
    _listBucketMetrics(host, port, bucketArr, timeRange,
        accessKey, secretKey, verbose, ssl);
}
