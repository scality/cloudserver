const assert = require('assert');

const { ConfigObject: Config } = require('../../../lib/Config');
const { createClients, getResponseKeyId } = require('../../../lib/kms/wrapper');
const { KmsType, KmsProtocol, makeBackend } = require('@scality/arsenal/build/lib/network/KMSInterface');

function createBackends(kms) {
    const config = new Config();
    config._parseKms({ kms });
    const { clientInstances, defaultInstance } = createClients(config.kms);
    const backends = {};
    Object.values(clientInstances).forEach(instance => {
        backends[instance.client.backend.provider] = instance.client.backend;
    });
    return { backends, defaultBackend: defaultInstance.client.backend };
}

describe('KMS getResponseKeyId', () => {
    const { backends, defaultBackend } = createBackends([
        {
            protocol: 'kmip',
            providerName: 'thales',
            default: true,
            transport: { tls: { host: 'kmip.example.com' } },
        },
        {
            protocol: 'kmip',
            providerName: 'thales2',
            transport: { tls: { host: 'kmip2.example.com' } },
        },
        {
            protocol: 'aws_kms',
            providerName: 'acme',
            endpoint: 'https://kms.example.com',
            ak: 'accessKey',
            sk: 'secretKey',
        },
    ]);
    const sseMigrationBackend = makeBackend(KmsType.internal, KmsProtocol.file, 'scality');
    const awsKeyArn = 'arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab';

    describe('with kmsHideScalityArn', () => {
        it('should remove the arn prefix of a key in the default KMS', () => {
            const key = `${defaultBackend.arnPrefix}54`;
            assert.strictEqual(getResponseKeyId(key, true, defaultBackend, defaultBackend), '54');
        });

        it('should return the arn of a key in another KMS', () => {
            const key = `${backends.thales2.arnPrefix}54`;
            assert.strictEqual(getResponseKeyId(key, true, defaultBackend, defaultBackend), key);
        });

        it('should return the scality arn of an AWS KMS key in another KMS', () => {
            const key = `${backends.acme.arnPrefix}${awsKeyArn}`;
            assert.strictEqual(getResponseKeyId(key, true, defaultBackend, defaultBackend), key);
        });

        it('should return only the AWS arn of a key in a default AWS KMS', () => {
            const key = `${backends.acme.arnPrefix}${awsKeyArn}`;
            assert.strictEqual(getResponseKeyId(key, true, backends.acme, backends.acme), awsKeyArn);
        });

        it('should return the arn of a key in a KMS that is not configured', () => {
            const key = `${makeBackend(KmsType.external, KmsProtocol.kmip, 'unconfigured').arnPrefix}54`;
            assert.strictEqual(getResponseKeyId(key, true, defaultBackend, defaultBackend), key);
        });

        it('should return a key stored without arn prefix as is without SSE migration', () => {
            assert.strictEqual(getResponseKeyId('abcd', true, defaultBackend, defaultBackend), 'abcd');
        });

        it('should add the SSE-migration KMS arn prefix to a key stored without arn prefix', () => {
            assert.strictEqual(
                getResponseKeyId('abcd', true, defaultBackend, sseMigrationBackend),
                `${sseMigrationBackend.arnPrefix}abcd`,
            );
        });

        it('should return the arn of a key in the SSE-migration KMS', () => {
            const key = `${sseMigrationBackend.arnPrefix}abcd`;
            assert.strictEqual(getResponseKeyId(key, true, defaultBackend, sseMigrationBackend), key);
        });
    });

    describe('without kmsHideScalityArn', () => {
        it('should return the arn of a key in the default KMS', () => {
            const key = `${defaultBackend.arnPrefix}54`;
            assert.strictEqual(getResponseKeyId(key, false, defaultBackend, defaultBackend), key);
        });

        it('should return the arn of a key in another KMS', () => {
            const key = `${backends.thales2.arnPrefix}54`;
            assert.strictEqual(getResponseKeyId(key, false, defaultBackend, defaultBackend), key);
        });

        it('should return a key stored without arn prefix as is without SSE migration', () => {
            assert.strictEqual(getResponseKeyId('abcd', false, defaultBackend, defaultBackend), 'abcd');
        });

        it('should add the SSE-migration KMS arn prefix to a key stored without arn prefix', () => {
            assert.strictEqual(
                getResponseKeyId('abcd', false, defaultBackend, sseMigrationBackend),
                `${sseMigrationBackend.arnPrefix}abcd`,
            );
        });
    });
});
