const assert = require('assert');
const sinon = require('sinon');

const logger = require('../../../lib/utilities/logger');
const { createClients } = require('../../../lib/kms/wrapper');
const { kmsTestEntries, parseKmsEntries } = require('../helpers');

describe('KMS createClients', () => {
    afterEach(() => {
        sinon.restore();
    });

    it('should build a client for every entry, keyed by its backend', () => {
        const entries = parseKmsEntries([kmsTestEntries.kmip, kmsTestEntries.awsKms, kmsTestEntries.file]);

        const { clientInstances, defaultInstance } = createClients(entries);

        assert.deepStrictEqual(
            Object.entries(clientInstances).map(([identifier, instance]) => [identifier, instance.implName]),
            [
                ['external:kmip:thales', 'kmip'],
                ['external:aws_kms:acme', 'aws'],
                ['internal:file:scality', 'fileKms'],
            ],
        );
        assert.strictEqual(defaultInstance, clientInstances['external:kmip:thales']);
    });

    it('should build a cluster client for a kmip entry with several hosts', () => {
        const entries = parseKmsEntries([
            {
                protocol: 'kmip',
                providerName: 'thales',
                default: true,
                transport: [{ tls: { host: 'kmip1' } }, { tls: { host: 'kmip2' } }],
            },
        ]);

        const { defaultInstance } = createClients(entries);

        assert.strictEqual(defaultInstance.client.clients.length, 2);
        assert.strictEqual(defaultInstance.client.retries, 1);
        assert.strictEqual(defaultInstance.client.backend.arnPrefix, 'arn:scality:kms:external:kmip:thales:key/');
    });

    it('should use the file backend for a SafeNet entry when the SafeNet client module is not installed', () => {
        const warn = sinon.stub(logger, 'warn');
        const entries = parseKmsEntries([
            { protocol: 'scality', providerName: 'safenet', default: true, userName: 'user', password: 'password' },
        ]);

        const { clientInstances, defaultInstance } = createClients(entries);

        assert.deepStrictEqual(Object.keys(clientInstances), ['internal:file:scality']);
        assert.strictEqual(defaultInstance.implName, 'fileKms');
        assert(warn.calledOnceWith('scality kms unavailable. Using file kms backend unless mem specified.'));
    });
});
