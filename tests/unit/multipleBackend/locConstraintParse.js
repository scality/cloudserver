const assert = require('assert');
const parseLC = require('../../../lib/data/locationConstraintParser');
const AwsClient = require('../../../lib/data/external/AwsClient');
const inMemory = require('../../../lib/data/in_memory/backend').backend;
const DataFileInterface = require('../../../lib/data/file/backend');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsHttpLocation = 'awsbackendhttp';
const clients = parseLC();

describe('locationConstraintParser', () => {
    it('should return object containing mem object', () => {
        assert.notStrictEqual(clients[memLocation], undefined);
        assert.strictEqual(typeof clients[memLocation], 'object');
        assert.deepEqual(clients[memLocation], inMemory);
    });
    it('should return object containing file object', () => {
        assert.notStrictEqual(clients[fileLocation], undefined);
        assert(clients[fileLocation] instanceof DataFileInterface);
    });

    it('should set correct options for https(default) aws_s3 type loc', () => {
        const client = clients[awsLocation];
        assert.notStrictEqual(client, undefined);
        assert(client instanceof AwsClient);
        assert.strictEqual(client._s3Params.sslEnabled, true);
        assert.strictEqual(client._s3Params.httpOptions.agent.protocol,
            'https:');
        assert.strictEqual(client._s3Params.httpOptions.agent.keepAlive, true);
        assert.strictEqual(client._s3Params.signatureVersion, 'v4');
    });

    it('should set correct options for http aws_s3 type location', () => {
        const client = clients[awsHttpLocation];
        assert.notStrictEqual(client, undefined);
        assert(client instanceof AwsClient);
        assert.strictEqual(client._s3Params.sslEnabled, false);
        assert.strictEqual(client._s3Params.httpOptions.agent.protocol,
            'http:');
        assert.strictEqual(client._s3Params.httpOptions.agent.keepAlive, true);
        assert.strictEqual(client._s3Params.signatureVersion, 'v2');
    });
});
