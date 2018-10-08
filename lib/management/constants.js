/* From RFC6455 which defines some reserved status code ranges, the range
 * 4000-4999 are for private use. */
const WS_STATUS_IDLE = {
    code: 4000,
    reason: 'does not reply to ping before timeout',
};

const CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS =
    process.env.MANAGEMENT_CHECK_CLIENT_FQCY_MS || 15000;

const endpointPath = 'api/v1/instance';

const managementEndpointRoot =
    process.env.MANAGEMENT_ENDPOINT ||
    'https://api.zenko.io';
const managementEndpoint = `${managementEndpointRoot}/${endpointPath}`;

const pushEndpointRoot =
    process.env.PUSH_ENDPOINT ||
    'https://push.api.zenko.io';
const pushEndpoint = `${pushEndpointRoot}/${endpointPath}`;


module.exports = {
    WS_STATUS_IDLE,
    CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS,
    managementEndpoint,
    pushEndpoint,
};
