const assert = require('assert');
const { KmsType, KmsProtocol, makeBackend } = require('arsenal/build/lib/network/KMSInterface');
const { belongsToMigrationSource } = require('../../../lib/kms/wrapper');

const internal = makeBackend(KmsType.internal, KmsProtocol.file, 'scality');
const kmip = makeBackend(KmsType.external, KmsProtocol.kmip, 'thales');
const awsKms = makeBackend(KmsType.external, KmsProtocol.aws_kms, 'localaws');
const keyId = '891a72205529b8a7531362d59d485f2fd182bb17cc7b07b2613fb01a2f966d56';

describe('belongsToMigrationSource', () => {
    [
        {
            desc: 'a key ID without ARN prefix',
            storedKey: keyId,
            migrationBackend: internal,
            expected: true,
        },
        {
            desc: 'a key ARN of the SSE-migration KMS',
            storedKey: `${internal.arnPrefix}${keyId}`,
            migrationBackend: internal,
            expected: true,
        },
        {
            desc: 'a key ARN of a KMIP KMS',
            storedKey: `${kmip.arnPrefix}931`,
            migrationBackend: internal,
            expected: false,
        },
        {
            desc: 'a key ARN of an AWS KMS',
            storedKey: `${awsKms.arnPrefix}36d31b52-c5a7-4e1d-9f0b-2a8c3e6d7f41`,
            migrationBackend: internal,
            expected: false,
        },
        {
            desc: 'a key ARN of the internal KMS without SSE migration',
            storedKey: `${internal.arnPrefix}${keyId}`,
            migrationBackend: undefined,
            expected: false,
        },
    ].forEach(({ desc, storedKey, migrationBackend, expected }) =>
        it(`should return ${expected} for ${desc}`, () => {
            assert.strictEqual(belongsToMigrationSource(storedKey, migrationBackend), expected);
        }),
    );
});
