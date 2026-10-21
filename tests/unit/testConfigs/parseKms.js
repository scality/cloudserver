const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { ConfigObject: Config } = require('../../../lib/Config');

const kmipEntry = {
    protocol: 'kmip',
    providerName: 'thales',
    default: true,
    transport: { tls: { host: 'kmip.example.com', port: 5696 } },
};

const awsEntry = {
    protocol: 'aws_kms',
    providerName: 'acme',
    endpoint: 'https://kms.example.com',
    ak: 'accessKey',
    sk: 'secretKey',
};

const fileEntry = {
    protocol: 'file',
    providerName: 'scality',
};

const safeNetEntry = {
    protocol: 'scality',
    providerName: 'safenet',
    default: true,
    userName: 'user',
    password: 'password',
};

const kmsEnvironmentVariables = [
    'S3KMS',
    'S3BACKEND',
    'S3KMIP_PIPELINE_DEPTH',
    'S3KMIP_PORT',
    'S3KMIP_HOSTS',
    'S3KMIP_HOST',
    'S3KMIP_KEY',
    'S3KMIP_CERT',
    'S3KMIP_CA',
    'S3KMIP_COMPOUND_CREATE',
    'S3KMIP_BUCKET_ATTRIBUTE_NAME',
];

describe('parseKms Function', () => {
    let configInstance;
    let savedEnvironment;

    beforeEach(() => {
        configInstance = new Config();
        savedEnvironment = {};
        kmsEnvironmentVariables.forEach(name => {
            savedEnvironment[name] = process.env[name];
            delete process.env[name];
        });
    });

    afterEach(() => {
        kmsEnvironmentVariables.forEach(name => {
            if (savedEnvironment[name] === undefined) {
                delete process.env[name];
            } else {
                process.env[name] = savedEnvironment[name];
            }
        });
    });

    describe('with a kms array', () => {
        it('should parse a kmip entry', () => {
            configInstance._parseKms({ kms: [kmipEntry] });
            assert.strictEqual(configInstance.kms.length, 1);
            const [entry] = configInstance.kms;
            assert.strictEqual(entry.protocol, 'kmip');
            assert.strictEqual(entry.providerName, 'thales');
            assert.strictEqual(entry.default, true);
            assert.strictEqual(entry.settings.providerName, 'thales');
            assert.strictEqual(entry.settings.transport.tls.host, 'kmip.example.com');
            assert.strictEqual(entry.settings.transport.tls.port, 5696);
            assert.strictEqual(entry.settings.transport.pipelineDepth, 8);
            assert.deepStrictEqual(entry.settings.client, {
                compoundCreateActivate: false,
                bucketNameAttributeName: '',
            });
        });

        it('should parse a kmip entry with a cluster transport', () => {
            const kmipCluster = {
                ...kmipEntry,
                transport: [{ tls: { host: 'kmip1' } }, { tls: { host: 'kmip2' } }],
            };
            configInstance._parseKms({ kms: [kmipCluster] });
            const { settings } = configInstance.kms[0];
            assert.deepStrictEqual(
                settings.transport.map(transport => transport.tls.host),
                ['kmip1', 'kmip2'],
            );
            assert.strictEqual(settings.retries, 1);
        });

        it('should parse an aws_kms entry', () => {
            configInstance._parseKms({ kms: [{ ...awsEntry, default: true }] });
            const [entry] = configInstance.kms;
            assert.strictEqual(entry.protocol, 'aws_kms');
            assert.deepStrictEqual(entry.settings, {
                providerName: 'acme',
                endpoint: 'https://kms.example.com',
                ak: 'accessKey',
                sk: 'secretKey',
            });
        });

        it('should parse a file entry', () => {
            configInstance._parseKms({ kms: [{ ...fileEntry, default: true }] });
            assert.deepStrictEqual(configInstance.kms, [
                { protocol: 'file', providerName: 'scality', default: true, settings: {} },
            ]);
        });

        it('should parse a SafeNet entry without its protocol and default fields', () => {
            configInstance._parseKms({ kms: [safeNetEntry] });
            assert.deepStrictEqual(configInstance.kms[0].settings, {
                providerName: 'safenet',
                userName: 'user',
                password: 'password',
            });
        });

        it('should parse several entries and mark the default one', () => {
            configInstance._parseKms({ kms: [awsEntry, kmipEntry, fileEntry] });
            assert.deepStrictEqual(
                configInstance.kms.map(entry => [entry.protocol, entry.default]),
                [
                    ['aws_kms', false],
                    ['kmip', true],
                    ['file', false],
                ],
            );
            assert.strictEqual(configInstance.getDefaultKms().providerName, 'thales');
        });

        it('should not select the KMS from S3KMS or S3BACKEND and record S3KMS as ignored', () => {
            process.env.S3KMS = 'file';
            process.env.S3BACKEND = 'mem';
            configInstance._parseKms({ kms: [kmipEntry] });
            assert.strictEqual(configInstance.getDefaultKms().protocol, 'kmip');
            assert.deepStrictEqual(configInstance.kmsIgnoredEnvironment, ['S3KMS']);
        });

        it('should record no ignored environment variable when none is set', () => {
            configInstance._parseKms({ kms: [kmipEntry] });
            assert.deepStrictEqual(configInstance.kmsIgnoredEnvironment, []);
        });

        [
            ['kmip', { kmip: { providerName: 'thales' } }],
            ['kmsAWS', { kmsAWS: { providerName: 'acme' } }],
        ].forEach(([key, config]) => {
            it(`should reject the ${key} key`, () => {
                assert.throws(
                    () => configInstance._parseKms(config),
                    /kmip and kmsAWS are replaced by entries of the kms array/,
                );
            });
        });

        it('should reject a kms object', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: { providerName: 'safenet', userName: 'u', password: 'p' } }),
                /kms must be an array of KMS entries/,
            );
        });

        it('should reject an empty kms array', () => {
            assert.throws(() => configInstance._parseKms({ kms: [] }), /kms must have at least one entry/);
        });

        it('should reject an entry that is not an object', () => {
            assert.throws(() => configInstance._parseKms({ kms: ['kmip'] }), /each kms entry must be an object/);
        });

        ['mem', 'cdmi', 'aws', undefined].forEach(protocol => {
            it(`should reject the protocol ${protocol}`, () => {
                assert.throws(
                    () => configInstance._parseKms({ kms: [{ ...kmipEntry, protocol }] }),
                    new RegExp(`protocol must be one of file, kmip, aws_kms, scality, got "${protocol}"`),
                );
            });
        });

        it('should reject an entry without providerName', () => {
            const { providerName, ...entry } = kmipEntry; // eslint-disable-line no-unused-vars
            assert.throws(() => configInstance._parseKms({ kms: [entry] }), /kmip kms entry must define providerName/);
        });

        it('should reject an invalid providerName', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...kmipEntry, providerName: 'Thales' }] }),
                /providerName must be lowercase alphanumeric only/,
            );
        });

        it('should reject a providerName used twice', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [kmipEntry, { ...awsEntry, providerName: 'thales' }] }),
                /providerName "thales" is not unique/,
            );
        });

        it('should reject a file entry with a providerName other than scality', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...fileEntry, providerName: 'local', default: true }] }),
                /file kms entry providerName must be "scality"/,
            );
        });

        it('should reject a default that is not a boolean', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...kmipEntry, default: 'true' }] }),
                /kms entry default must be a boolean/,
            );
        });

        it('should reject entries without a default', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [awsEntry, fileEntry] }),
                /exactly one kms entry must have "default": true, found 0/,
            );
        });

        it('should reject entries with two defaults', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [kmipEntry, { ...awsEntry, default: true }] }),
                /exactly one kms entry must have "default": true, found 2/,
            );
        });

        it('should reject a kmip entry without transport', () => {
            const { transport, ...entry } = kmipEntry; // eslint-disable-line no-unused-vars
            assert.throws(() => configInstance._parseKms({ kms: [entry] }), /KMIP transport must define tls.host/);
        });

        it('should reject a kmip transport without host', () => {
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...kmipEntry, transport: { tls: { port: 5696 } } }] }),
                /KMIP transport must define tls.host/,
            );
        });

        it('should reject a kmip cluster transport without host', () => {
            const transport = [{ tls: { host: 'kmip1' } }, { pipelineDepth: 8 }];
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...kmipEntry, transport }] }),
                /KMIP transport must define tls.host/,
            );
        });

        it('should reject an aws_kms entry without endpoint', () => {
            const { endpoint, ...entry } = awsEntry; // eslint-disable-line no-unused-vars
            assert.throws(
                () => configInstance._parseKms({ kms: [{ ...entry, default: true }] }),
                /endpoint must be defined in the aws_kms KMS entry/,
            );
        });

        it('should reject a SafeNet entry without userName', () => {
            const { userName, ...entry } = safeNetEntry; // eslint-disable-line no-unused-vars
            assert.throws(
                () => configInstance._parseKms({ kms: [entry] }),
                /userName of the SafeNet KMS entry must be a string/,
            );
        });
    });

    describe('without a kms array', () => {
        it('should select the file KMS when no environment variable is set', () => {
            configInstance._parseKms({});
            assert.deepStrictEqual(configInstance.kms, [{ protocol: 'file', default: true, settings: {} }]);
            assert.deepStrictEqual(configInstance.kmsIgnoredEnvironment, []);
        });

        [
            ['mem', 'mem'],
            ['file', 'file'],
            ['cdmi', 'file'],
            ['scality', 'scality'],
        ].forEach(([backend, protocol]) => {
            it(`should select the ${protocol} KMS with S3BACKEND=${backend}`, () => {
                process.env.S3BACKEND = backend;
                configInstance._parseKms({});
                assert.strictEqual(configInstance.getDefaultKms().protocol, protocol);
            });
        });

        it('should select the KMS from S3KMS over S3BACKEND', () => {
            process.env.S3BACKEND = 'mem';
            process.env.S3KMS = 'file';
            configInstance._parseKms({});
            assert.strictEqual(configInstance.getDefaultKms().protocol, 'file');
        });

        ['kmip', 'aws'].forEach(name => {
            it(`should reject S3KMS=${name}`, () => {
                process.env.S3KMS = name;
                assert.throws(
                    () => configInstance._parseKms({}),
                    new RegExp(`S3KMS=${name} needs an entry with a providerName in the kms array`),
                );
            });
        });

        it('should reject an unknown S3KMS', () => {
            process.env.S3KMS = 'unknown';
            assert.throws(() => configInstance._parseKms({}), /S3KMS should be one of mem\/file\/cdmi\/scality/);
        });
    });

    describe('with S3KMIP_* environment variables', () => {
        const kmipEntryWithoutTransport = { protocol: 'kmip', providerName: 'thales', default: true };
        let tlsDirectory;

        beforeEach(() => {
            tlsDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'parseKms-'));
        });

        afterEach(() => {
            fs.rmSync(tlsDirectory, { recursive: true, force: true });
        });

        it('should take the transport and client of the default kmip entry from them', () => {
            process.env.S3KMIP_HOSTS = 'kmip.example.com';
            process.env.S3KMIP_PORT = '5697';
            process.env.S3KMIP_PIPELINE_DEPTH = '4';
            process.env.S3KMIP_COMPOUND_CREATE = 'true';
            process.env.S3KMIP_BUCKET_ATTRIBUTE_NAME = 'bucket';
            configInstance._parseKms({ kms: [kmipEntryWithoutTransport] });
            const { settings } = configInstance.getDefaultKms();
            assert.strictEqual(settings.transport.tls.host, 'kmip.example.com');
            assert.strictEqual(settings.transport.tls.port, '5697');
            assert.strictEqual(settings.transport.pipelineDepth, '4');
            assert.deepStrictEqual(settings.client, {
                compoundCreateActivate: true,
                bucketNameAttributeName: 'bucket',
            });
        });

        it('should take the host from S3KMIP_HOST', () => {
            process.env.S3KMIP_HOST = 'kmip.example.com';
            configInstance._parseKms({ kms: [kmipEntryWithoutTransport] });
            assert.strictEqual(configInstance.getDefaultKms().settings.transport.tls.host, 'kmip.example.com');
        });

        it('should let the settings of the default kmip entry override them', () => {
            process.env.S3KMIP_HOSTS = 'env.example.com';
            process.env.S3KMIP_PORT = '5697';
            process.env.S3KMIP_BUCKET_ATTRIBUTE_NAME = 'envBucket';
            configInstance._parseKms({
                kms: [
                    {
                        ...kmipEntryWithoutTransport,
                        client: { bucketNameAttributeName: 'entryBucket' },
                        transport: { tls: { host: 'entry.example.com', port: 5698 } },
                    },
                ],
            });
            const { settings } = configInstance.getDefaultKms();
            assert.strictEqual(settings.transport.tls.host, 'entry.example.com');
            assert.strictEqual(settings.transport.tls.port, 5698);
            assert.strictEqual(settings.client.bucketNameAttributeName, 'entryBucket');
        });

        it('should keep S3KMIP_COMPOUND_CREATE=true when the entry sets compoundCreateActivate to false', () => {
            process.env.S3KMIP_COMPOUND_CREATE = 'true';
            configInstance._parseKms({
                kms: [{ ...kmipEntry, client: { compoundCreateActivate: false } }],
            });
            assert.strictEqual(configInstance.getDefaultKms().settings.client.compoundCreateActivate, true);
        });

        it('should drop S3KMIP_CA when the entry sets tls without ca', () => {
            const caFile = path.join(tlsDirectory, 'ca.pem');
            fs.writeFileSync(caFile, 'ca');
            process.env.S3KMIP_CA = caFile;
            configInstance._parseKms({
                kms: [{ ...kmipEntryWithoutTransport, transport: { tls: { host: 'kmip1' } } }],
            });
            assert.strictEqual(configInstance.getDefaultKms().settings.transport.tls.ca, undefined);
        });

        it('should load S3KMIP_CA when the entry has no tls', () => {
            const caFile = path.join(tlsDirectory, 'ca.pem');
            fs.writeFileSync(caFile, 'ca');
            process.env.S3KMIP_HOSTS = 'kmip1';
            process.env.S3KMIP_CA = caFile;
            configInstance._parseKms({ kms: [kmipEntryWithoutTransport] });
            assert.deepStrictEqual(configInstance.getDefaultKms().settings.transport.tls.ca, [Buffer.from('ca')]);
        });

        it('should apply to every transport of a default kmip cluster', () => {
            process.env.S3KMIP_PIPELINE_DEPTH = '4';
            configInstance._parseKms({
                kms: [
                    {
                        ...kmipEntryWithoutTransport,
                        transport: [{ tls: { host: 'kmip1' } }, { tls: { host: 'kmip2' } }],
                    },
                ],
            });
            assert.deepStrictEqual(
                configInstance.getDefaultKms().settings.transport.map(transport => transport.pipelineDepth),
                ['4', '4'],
            );
        });

        it('should not apply to a kmip entry that is not the default', () => {
            process.env.S3KMIP_COMPOUND_CREATE = 'true';
            process.env.S3KMIP_PIPELINE_DEPTH = '4';
            configInstance._parseKms({
                kms: [
                    { ...awsEntry, default: true },
                    { ...kmipEntry, default: false },
                ],
            });
            const kmipSettings = configInstance.kms.find(entry => entry.protocol === 'kmip').settings;
            assert.strictEqual(kmipSettings.client.compoundCreateActivate, false);
            assert.strictEqual(kmipSettings.transport.pipelineDepth, 8);
        });

        it('should not give a host to a kmip entry that is not the default', () => {
            process.env.S3KMIP_HOSTS = 'kmip.example.com';
            assert.throws(
                () =>
                    configInstance._parseKms({
                        kms: [
                            { ...awsEntry, default: true },
                            { protocol: 'kmip', providerName: 'thales' },
                        ],
                    }),
                /KMIP transport must define tls.host/,
            );
        });
    });

    describe('with sseMigration from an external KMS', () => {
        it('should name the protocol of the default KMS', () => {
            configInstance._parseKms({ kms: [{ ...fileEntry, default: true }] });
            const sseMigration = {
                previousKeyType: 'external',
                previousKeyProtocol: 'kmip',
                previousKeyProvider: 'thales',
            };
            assert.throws(
                () => configInstance._sseMigration({ sseMigration }),
                /can't migrate to "internal" KMS provider file/,
            );
        });
    });
});
