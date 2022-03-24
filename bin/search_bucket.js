#!/bin/sh
// 2>/dev/null ; exec "$(which nodejs 2>/dev/null || which node)" "$0" "$@"
'use strict'; // eslint-disable-line strict

const { auth } = require('arsenal');
const commander = require('commander');

const http = require('http');
const https = require('https');
const logger = require('../lib/utilities/logger');

function _performSearch(host,
                        port,
                        bucketName,
                        query,
                        listVersions,
                        accessKey,
                        secretKey,
                        sessionToken,
                        verbose, ssl) {
    const escapedSearch = encodeURIComponent(query);
    const options = {
        host,
        port,
        method: 'GET',
        path: `/${bucketName}/?search=${escapedSearch}${listVersions ? '&&versions' : ''}`,
        headers: {
            'Content-Length': 0,
        },
        rejectUnauthorized: false,
        versions: '',
    };
    if (sessionToken) {
        options.headers['x-amz-security-token'] = sessionToken;
    }
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
                logger.info('Success');
                process.stdout.write(body.join(''));
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
    // generateV4Headers exepects request object with path that does not
    // include query
    request.path = `/${bucketName}`;
    const requestData = listVersions ? { search: query, versions: '' } : { search: query };
    auth.client.generateV4Headers(request, requestData, accessKey, secretKey, 's3');
    request.path = `/${bucketName}?search=${escapedSearch}${listVersions ? '&&versions' : ''}`;
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }
    request.end();
}

/**
 * This function is used as a binary to send a request to S3 to perform a
 * search on the objects in a bucket
 *
 * @return {undefined}
 */
function searchBucket() {
    // TODO: Include other bucket listing possible query params?
    commander
        .version('0.0.1')
        .option('-a, --access-key <accessKey>', 'Access key id')
        .option('-k, --secret-key <secretKey>', 'Secret access key')
        .option('-t, --session-token <sessionToken>', 'Session token')
        .option('-b, --bucket <bucket>', 'Name of the bucket')
        .option('-q, --query <query>', 'Search query')
        .option('-h, --host <host>', 'Host of the server')
        .option('-p, --port <port>', 'Port of the server')
        .option('-s', '--ssl', 'Enable ssl')
        .option('-l, --list-versions', 'List all versions of the objects that meet the search query, ' +
            'otherwise only list the latest version')
        .option('-v, --verbose')
        .parse(process.argv);
    const { host, port, accessKey, secretKey, sessionToken, bucket, query, listVersions, verbose, ssl } =
        commander;

    if (!host || !port || !accessKey || !secretKey || !bucket || !query) {
        logger.error('missing parameter');
        commander.outputHelp();
        process.exit(1);
    }

    _performSearch(host, port, bucket, query, listVersions, accessKey, secretKey, sessionToken, verbose,
        ssl);
}

searchBucket();
