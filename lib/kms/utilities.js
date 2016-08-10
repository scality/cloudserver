import { auth } from 'arsenal';
import commander from 'commander';
import http from 'http';
import https from 'https';
import { logger } from '../utilities/logger';

function _createEncryptedBucket(host,
                                port,
                                bucketName,
                                accessKey,
                                secretKey,
                                verbose, ssl) {
    const options = {
        host,
        port,
        method: 'PUT',
        path: `/${bucketName}/`,
        headers: {
            'Content-Length': 0,
            'x-amz-scal-server-side-encryption': 'AES256',
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
            if (response.statusCode >= 200 && response.statusCode < 300) {
                logger.info('Success', {
                    body: verbose ? body.join('') : undefined,
                });
                process.exit(0);
            } else {
                logger.error('request failed with HTTP Status ', {
                    statusCode: response.statusCode,
                    body: body.join(''),
                });
                process.exit(1);
            }
        });
    });

    auth.generateV4Headers(request, '', accessKey, secretKey, 's3');
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }
    request.end();
}

/**
 * This function is used as a binary to send a request to S3 and create an
 * encrypted bucket, because most of the s3 tools don't support custom
 * headers
 *
 * @return {undefined}
 */
export function createEncryptedBucket() {
    commander
        .version('0.0.1')
        .option('-a, --access-key <accessKey>', 'Access key id')
        .option('-k, --secret-key <secretKey>', 'Secret access key')
        .option('-b, --bucket <bucket>', 'Name of the bucket')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('-s', '--ssl', 'Enable ssl')
        .option('-v, --verbose')
        .parse(process.argv);

    const { host, port, accessKey, secretKey, bucket, verbose, ssl } =
        commander;

    if (!host || !port || !accessKey || !secretKey || !bucket) {
        logger.error('missing parameter');
        commander.outputHelp();
        process.exit(1);
    }

    _createEncryptedBucket(host, port, bucket, accessKey, secretKey, verbose,
        ssl);
}
