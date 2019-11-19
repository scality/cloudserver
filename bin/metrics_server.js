#!/usr/bin/env node
'use strict'; // eslint-disable-line strict

const {
    startWSManagementClient,
    startPushConnectionHealthCheckServer,
} = require('../lib/management/push');

const logger = require('../lib/utilities/logger');

const {
    PUSH_ENDPOINT: pushEndpoint,
    INSTANCE_ID: instanceId,
    MANAGEMENT_TOKEN: managementToken,
} = process.env;

if (!pushEndpoint) {
    logger.error('missing push endpoint env var');
    process.exit(1);
}

if (!instanceId) {
    logger.error('missing instance id env var');
    process.exit(1);
}

if (!managementToken) {
    logger.error('missing management token env var');
    process.exit(1);
}

startPushConnectionHealthCheckServer(err => {
    if (err) {
        logger.error('could not start healthcheck server', { error: err });
        process.exit(1);
    }
    const url = `${pushEndpoint}/${instanceId}/ws?metrics=1`;
    startWSManagementClient(url, managementToken, err => {
        if (err) {
            logger.error('connection failed, exiting', { error: err });
            process.exit(1);
        }
        logger.info('no more connection, exiting');
        process.exit(0);
    });
});
