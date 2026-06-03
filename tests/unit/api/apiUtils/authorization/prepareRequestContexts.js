const assert = require('assert');
const DummyRequest = require('../../../DummyRequest');
const prepareRequestContexts = require('../../../../../lib/api/apiUtils/authorization/prepareRequestContexts.js');

const makeRequest = (headers, query) =>
    new DummyRequest({
        headers,
        url: '/',
        parsedHost: 'localhost',
        socket: {},
        query,
    });
const sourceBucket = 'bucketsource';
const sourceObject = 'objectsource';
const sourceVersionId = 'vid1';

describe('prepareRequestContexts', () => {
    it('should return s3:DeleteObject if multiObjectDelete method', () => {
        const apiMethod = 'multiObjectDelete';
        const request = makeRequest();
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:DeleteObject';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it(
        'should return s3:PutObjectVersion request context action for objectPut method with x-scal-s3-version-id' +
            ' header',
        () => {
            const apiMethod = 'objectPut';
            const request = makeRequest({
                'x-scal-s3-version-id': 'vid',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObjectVersion';
            assert.strictEqual(results[0].getAction(), expectedAction);
        },
    );

    it(
        'should return s3:PutObjectVersion request context action for objectPut method with empty x-scal-s3-version-id' +
            ' header',
        () => {
            const apiMethod = 'objectPut';
            const request = makeRequest({
                'x-scal-s3-version-id': '',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObjectVersion';
            assert.strictEqual(results[0].getAction(), expectedAction);
        },
    );

    it('should return s3:PutObject request context action for objectPut method and no header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({});
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:PutObject';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it(
        'should return s3:PutObject and s3:PutObjectTagging actions for objectPut method with' +
            ' x-amz-tagging header',
        () => {
            const apiMethod = 'objectPut';
            const request = makeRequest({
                'x-amz-tagging': 'key1=value1&key2=value2',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 2);
            const expectedAction1 = 's3:PutObject';
            const expectedAction2 = 's3:PutObjectTagging';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
        },
    );

    it('should return s3:PutObject and s3:PutObjectAcl actions for objectPut method with ACL header', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-amz-acl': 'private',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 2);
        const expectedAction1 = 's3:PutObject';
        const expectedAction2 = 's3:PutObjectAcl';
        assert.strictEqual(results[0].getAction(), expectedAction1);
        assert.strictEqual(results[1].getAction(), expectedAction2);
    });

    it('should return s3:GetObject for headObject', () => {
        const apiMethod = 'objectHead';
        const request = makeRequest({});
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].getAction(), 's3:GetObject');
    });

    it('should return s3:GetObject and s3:GetObjectVersion for headObject', () => {
        const apiMethod = 'objectHead';
        const request = makeRequest({
            'x-amz-version-id': '0987654323456789',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 2);
        assert.strictEqual(results[0].getAction(), 's3:GetObject');
        assert.strictEqual(results[1].getAction(), 's3:GetObjectVersion');
    });

    it(
        'should return s3:GetObject and scality:GetObjectArchiveInfo for headObject ' +
            'with x-amz-scal-archive-info header',
        () => {
            const apiMethod = 'objectHead';
            const request = makeRequest({
                'x-amz-scal-archive-info': 'true',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 2);
            assert.strictEqual(results[0].getAction(), 's3:GetObject');
            assert.strictEqual(results[1].getAction(), 'scality:GetObjectArchiveInfo');
        },
    );

    it(
        'should return s3:GetObject, s3:GetObjectVersion and scality:GetObjectArchiveInfo ' +
            ' for headObject with x-amz-scal-archive-info header',
        () => {
            const apiMethod = 'objectHead';
            const request = makeRequest({
                'x-amz-version-id': '0987654323456789',
                'x-amz-scal-archive-info': 'true',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 3);
            assert.strictEqual(results[0].getAction(), 's3:GetObject');
            assert.strictEqual(results[1].getAction(), 's3:GetObjectVersion');
            assert.strictEqual(results[2].getAction(), 'scality:GetObjectArchiveInfo');
        },
    );

    it('should return s3:PutObjectRetention with header x-amz-object-lock-mode', () => {
        const apiMethod = 'objectPut';
        const request = makeRequest({
            'x-amz-object-lock-mode': 'GOVERNANCE',
            'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 2);
        const expectedAction1 = 's3:PutObject';
        const expectedAction2 = 's3:PutObjectRetention';
        assert.strictEqual(results[0].getAction(), expectedAction1);
        assert.strictEqual(results[1].getAction(), expectedAction2);
    });

    it(
        'should return s3:PutObjectRetention and s3:BypassGovernanceRetention for objectPut ' +
            'with header x-amz-bypass-governance-retention',
        () => {
            const apiMethod = 'objectPut';
            const request = makeRequest({
                'x-amz-object-lock-mode': 'GOVERNANCE',
                'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
                'x-amz-bypass-governance-retention': 'true',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 3);
            const expectedAction1 = 's3:PutObject';
            const expectedAction2 = 's3:PutObjectRetention';
            const expectedAction3 = 's3:BypassGovernanceRetention';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
            assert.strictEqual(results[2].getAction(), expectedAction3);
        },
    );

    it(
        'should return s3:PutObjectRetention and s3:BypassGovernanceRetention for objectPut ' +
            'with header x-amz-bypass-governance-retention with version id specified',
        () => {
            const apiMethod = 'objectPut';
            const request = makeRequest(
                {
                    'x-amz-object-lock-mode': 'GOVERNANCE',
                    'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
                    'x-amz-bypass-governance-retention': 'true',
                },
                {
                    versionId: 'vid1',
                },
            );
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 3);
            const expectedAction1 = 's3:PutObject';
            const expectedAction2 = 's3:PutObjectRetention';
            const expectedAction3 = 's3:BypassGovernanceRetention';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
            assert.strictEqual(results[2].getAction(), expectedAction3);
        },
    );

    it('should return s3:PutObjectRetention with header x-amz-object-lock-mode for objectPutRetention action', () => {
        const apiMethod = 'objectPutRetention';
        const request = makeRequest({
            'x-amz-object-lock-mode': 'GOVERNANCE',
            'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
        });
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        const expectedAction = 's3:PutObjectRetention';
        assert.strictEqual(results[0].getAction(), expectedAction);
    });

    it(
        'should return s3:PutObjectRetention and s3:BypassGovernanceRetention for objectPutRetention ' +
            'with header x-amz-bypass-governance-retention',
        () => {
            const apiMethod = 'objectPutRetention';
            const request = makeRequest({
                'x-amz-object-lock-mode': 'GOVERNANCE',
                'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
                'x-amz-bypass-governance-retention': 'true',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 2);
            const expectedAction1 = 's3:PutObjectRetention';
            const expectedAction2 = 's3:BypassGovernanceRetention';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
        },
    );

    it(
        'should return s3:PutObjectRetention and s3:BypassGovernanceRetention for objectPutRetention ' +
            'with header x-amz-bypass-governance-retention with version id specified',
        () => {
            const apiMethod = 'objectPutRetention';
            const request = makeRequest(
                {
                    'x-amz-object-lock-mode': 'GOVERNANCE',
                    'x-amz-object-lock-retain-until-date': '2021-12-31T23:59:59.000Z',
                    'x-amz-bypass-governance-retention': 'true',
                },
                {
                    versionId: 'vid1',
                },
            );
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 2);
            const expectedAction1 = 's3:PutObjectRetention';
            const expectedAction2 = 's3:BypassGovernanceRetention';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
        },
    );

    it('should return s3:DeleteObject for objectDelete method', () => {
        const apiMethod = 'objectDelete';
        const request = makeRequest();
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].getAction(), 's3:DeleteObject');
    });

    it('should return s3:DeleteObjectVersion for objectDelete method with version id specified', () => {
        const apiMethod = 'objectDelete';
        const request = makeRequest(
            {},
            {
                versionId: 'vid1',
            },
        );
        const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].getAction(), 's3:DeleteObjectVersion');
    });

    // Now it shuld include the bypass header if set
    it(
        'should return s3:DeleteObjectVersion and s3:BypassGovernanceRetention for objectDelete method ' +
            'with version id specified and x-amz-bypass-governance-retention header',
        () => {
            const apiMethod = 'objectDelete';
            const request = makeRequest(
                {
                    'x-amz-bypass-governance-retention': 'true',
                },
                {
                    versionId: 'vid1',
                },
            );
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 2);
            const expectedAction1 = 's3:DeleteObjectVersion';
            const expectedAction2 = 's3:BypassGovernanceRetention';
            assert.strictEqual(results[0].getAction(), expectedAction1);
            assert.strictEqual(results[1].getAction(), expectedAction2);
        },
    );

    // When there is no version ID, AWS does not return any error if the object
    // is locked, but creates a delete marker
    it(
        'should only return s3:DeleteObject for objectDelete method ' +
            'with x-amz-bypass-governance-retention header and no version id',
        () => {
            const apiMethod = 'objectDelete';
            const request = makeRequest({
                'x-amz-bypass-governance-retention': 'true',
            });
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:DeleteObject';
            assert.strictEqual(results[0].getAction(), expectedAction);
        },
    );

    ['initiateMultipartUpload', 'objectPutPart', 'completeMultipartUpload'].forEach(apiMethod => {
        it(
            `should return s3:PutObjectVersion request context action for ${apiMethod} method ` +
                'with x-scal-s3-version-id header',
            () => {
                const request = makeRequest({
                    'x-scal-s3-version-id': '',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 1);
                const expectedAction = 's3:PutObjectVersion';
                assert.strictEqual(results[0].getAction(), expectedAction);
            },
        );

        it(
            `should return s3:PutObjectVersion request context action for ${apiMethod} method` +
                'with empty x-scal-s3-version-id header',
            () => {
                const request = makeRequest({
                    'x-scal-s3-version-id': '',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 1);
                const expectedAction = 's3:PutObjectVersion';
                assert.strictEqual(results[0].getAction(), expectedAction);
            },
        );

        it(`should return s3:PutObject request context action for ${apiMethod} method and no header`, () => {
            const request = makeRequest({});
            const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

            assert.strictEqual(results.length, 1);
            const expectedAction = 's3:PutObject';
            assert.strictEqual(results[0].getAction(), expectedAction);
        });
    });

    describe('bucketGet', () => {
        describe('x-amz-optional-object-attributes header', () => {
            it('should request for specific permission if the header is set', () => {
                const apiMethod = 'bucketGet';
                const request = makeRequest({
                    'x-amz-optional-object-attributes': 'x-amz-meta-department',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 2);
                assert.strictEqual(results[0].getAction(), 's3:ListBucket');
                assert.strictEqual(results[1].getAction(), 'scality:ListBucketOptionalObjectAttributes');
            });

            it('should request for specific permission if the header is set with multiple value', () => {
                const apiMethod = 'bucketGet';
                const request = makeRequest({
                    'x-amz-optional-object-attributes': 'x-amz-meta-department,RestoreStatus',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 2);
                assert.strictEqual(results[0].getAction(), 's3:ListBucket');
                assert.strictEqual(results[1].getAction(), 'scality:ListBucketOptionalObjectAttributes');
            });

            it('should not request permission if the header contains only RestoreStatus', () => {
                const apiMethod = 'bucketGet';
                const request = makeRequest({
                    'x-amz-optional-object-attributes': 'RestoreStatus',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 1);
                assert.strictEqual(results[0].getAction(), 's3:ListBucket');
            });

            it('should not request permission if the header does not exists', () => {
                const apiMethod = 'bucketGet';
                const request = makeRequest({});
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 1);
                assert.strictEqual(results[0].getAction(), 's3:ListBucket');
            });
        });
    });

    describe('objectGetAttributes', () => {
        describe('x-amz-object-attributes header', () => {
            it('should include scality:GetObjectAttributes with x-amz-meta attribute', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest({
                    'x-amz-object-attributes': 'x-amz-meta-department',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 3);
                assert.strictEqual(results[0].getAction(), 's3:GetObject');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectAttributes');
                assert.strictEqual(results[2].getAction(), 'scality:GetObjectAttributesCustom');
            });

            it('should include scality:GetObjectAttributes with multiple attributes', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest({
                    'x-amz-object-attributes': 'x-amz-meta-department,ETag',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 3);
                assert.strictEqual(results[0].getAction(), 's3:GetObject');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectAttributes');
                assert.strictEqual(results[2].getAction(), 'scality:GetObjectAttributesCustom');
            });

            it('should not include scality:GetObjectAttributes with only RestoreStatus', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest({
                    'x-amz-object-attributes': 'RestoreStatus',
                });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 2);
                assert.strictEqual(results[0].getAction(), 's3:GetObject');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectAttributes');
            });

            it('should not include scality:GetObjectAttributes without header', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest({});
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 2);
                assert.strictEqual(results[0].getAction(), 's3:GetObject');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectAttributes');
            });
        });

        describe('versionId query param', () => {
            it('should return version-specific actions with versionId query param', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest({}, { versionId: '0987654323456789' });
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 2);
                assert.strictEqual(results[0].getAction(), 's3:GetObjectVersion');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectVersionAttributes');
            });

            it('should include scality:GetObjectAttributes with versionId query param and x-amz-meta', () => {
                const apiMethod = 'objectGetAttributes';
                const request = makeRequest(
                    { 'x-amz-object-attributes': 'x-amz-meta-department' },
                    { versionId: '0987654323456789' },
                );
                const results = prepareRequestContexts(apiMethod, request, sourceBucket, sourceObject, sourceVersionId);

                assert.strictEqual(results.length, 3);
                assert.strictEqual(results[0].getAction(), 's3:GetObjectVersion');
                assert.strictEqual(results[1].getAction(), 's3:GetObjectVersionAttributes');
                assert.strictEqual(results[2].getAction(), 'scality:GetObjectAttributesCustom');
            });
        });
    });
});
