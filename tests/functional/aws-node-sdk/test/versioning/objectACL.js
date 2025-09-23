const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    GetObjectAclCommand,
    PutObjectAclCommand,
    HeadObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
    checkOneVersion,
} = require('../../lib/utility/versioning-util.js');

const counter = 100;
let bucket;
const key = '/';
const invalidId = 'invalidIdWithMoreThan40BytesAndThatIsNotLongEnoughYet';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

class _Utils {
    constructor(s3) {
        this.s3 = s3;
    }

    static assertNoError(err, desc) {
        assert.strictEqual(err, null, `Unexpected err ${desc}: ${err}`);
    }

    // need a wrapper because sdk apparently does not include version id in
    // exposed data object for put/get acl methods
    async _wrapDataObject(method, params) {
        const Command = method === 'getObjectAcl' ? GetObjectAclCommand : PutObjectAclCommand;
        const data = await this.s3.send(new Command(params));
        
        let versionId = params.VersionId;
        
        if (!versionId) {
            // For non-version-specific ACL operations, we need to determine the latest version
            try {
                const headResult = await this.s3.send(new HeadObjectCommand({
                    Bucket: params.Bucket,
                    Key: params.Key
                }));
                versionId = headResult.VersionId;
            } catch {
                versionId = undefined; // Fallback
            }
        }
        
        const dataObj = Object.assign({
            VersionId: versionId,
        }, data);
        return dataObj;
    }

    async getObjectAcl(params) {
        return this._wrapDataObject('getObjectAcl', params);
    }

    async putObjectAcl(params) {
        return this._wrapDataObject('putObjectAcl', params);
    }

    async putAndGetAcl(cannedAcl, versionId, expected) {
        const params = {
            Bucket: bucket,
            Key: key,
            ACL: cannedAcl,
        };
        if (versionId) {
            params.VersionId = versionId;
        }
        
        try {
            const data = await this.putObjectAcl(params);
            if (expected.error) {
                // Should not reach here if error was expected
                assert.fail('Expected error but operation succeeded');
            }
            _Utils.assertNoError(null,
                `putting object acl with version id: ${versionId}`);
            assert.strictEqual(data.VersionId, expected.versionId,
                `expected version id '${expected.versionId}' in ` +
                `putacl res headers, got '${data.VersionId}' instead`);
        } catch (err) {
            if (expected.error) {
                assert.strictEqual(expected.error.code, err.Code);
                assert.strictEqual(expected.error.statusCode, err.$metadata.httpStatusCode);
            } else {
                throw err;
            }
        }
        
        delete params.ACL;
        
        try {
            const data = await this.getObjectAcl(params);
            if (expected.error) {
                assert.fail('Expected error but operation succeeded');
            }
            _Utils.assertNoError(null,
                `getting object acl with version id: ${versionId}`);
            assert.strictEqual(data.VersionId, expected.versionId,
                `expected version id '${expected.versionId}' in ` +
                `getacl res headers, got '${data.VersionId}'`);
            assert.strictEqual(data.Grants.length, 2);
        } catch (err) {
            if (expected.error) {
                assert.strictEqual(expected.error.code, err.Code);
                assert.strictEqual(expected.error.statusCode, err.$metadata.httpStatusCode);
            } else {
                throw err;
            }
        }
    }
}

function _testBehaviorVersioningEnabledOrSuspended(utils, versionIds) {
    const s3 = utils.s3;

    it('should return 405 MethodNotAllowed putting acl without ' +
    'version id if latest version is a delete marker', async () => {
        const aclParams = {
            Bucket: bucket,
            Key: key,
            ACL: 'public-read-write',
        };
        const data = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(data.DeleteMarker, true);
        assert(data.VersionId);
        
        try {
            await utils.putObjectAcl(aclParams);
            assert.fail('Expected error but operation succeeded');
        } catch (err) {
            assert(err);
            assert.strictEqual(err.Code, 'MethodNotAllowed');
            assert.strictEqual(err.$metadata.httpStatusCode, 405);
        }
    });

    it('should return 405 MethodNotAllowed putting acl with ' +
    'version id if version specified is a delete marker', async () => {
        const aclParams = {
            Bucket: bucket,
            Key: key,
            ACL: 'public-read-write',
        };
        const data = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(data.DeleteMarker, true);
        assert(data.VersionId);
        aclParams.VersionId = data.VersionId;
        
        try {
            await utils.putObjectAcl(aclParams);
            assert.fail('Expected error but operation succeeded');
        } catch (err) {
            assert(err);
            assert.strictEqual(err.Code, 'MethodNotAllowed');
            assert.strictEqual(err.$metadata.httpStatusCode, 405);
        }
    });

    it('should return 404 NoSuchKey getting acl without ' +
    'version id if latest version is a delete marker', async () => {
        const aclParams = {
            Bucket: bucket,
            Key: key,
        };
        const data = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(data.DeleteMarker, true);
        assert(data.VersionId);
        
        try {
            await utils.getObjectAcl(aclParams);
            assert.fail('Expected error but operation succeeded');
        } catch (err) {
            assert(err);
            assert.strictEqual(err.Code, 'NoSuchKey');
            assert.strictEqual(err.$metadata.httpStatusCode, 404);
        }
    });

    it('should return 405 MethodNotAllowed getting acl with ' +
    'version id if version specified is a delete marker', async () => {
        const latestVersion = versionIds[versionIds.length - 1];
        const aclParams = {
            Bucket: bucket,
            Key: key,
            VersionId: latestVersion,
        };
        const data = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
        assert.strictEqual(data.DeleteMarker, true);
        assert(data.VersionId);
        aclParams.VersionId = data.VersionId;
        
        try {
            await utils.getObjectAcl(aclParams);
            assert.fail('Expected error but operation succeeded');
        } catch (err) {
            assert(err);
            assert.strictEqual(err.Code, 'MethodNotAllowed');
            assert.strictEqual(err.$metadata.httpStatusCode, 405);
        }
    });

    it('non-version specific put and get ACL should target latest ' +
    'version AND return version ID in response headers', async () => {
        const latestVersion = versionIds[versionIds.length - 1];
        const expectedRes = { versionId: latestVersion };
        await utils.putAndGetAcl('public-read', undefined, expectedRes);
    });

    it('version specific put and get ACL should return version ID ' +
    'in response headers', async () => {
        const firstVersion = versionIds[0];
        const expectedRes = { versionId: firstVersion };
        await utils.putAndGetAcl('public-read', firstVersion, expectedRes);
    });

    it('version specific put and get ACL (version id = "null") ' +
    'should return version ID ("null") in response headers', async () => {
        const expectedRes = { versionId: 'null' };
        await utils.putAndGetAcl('public-read', 'null', expectedRes);
    });
}

describe('versioned put and get object acl ::', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const utils = new _Utils(s3);

        beforeEach(async () => {
            bucket = `versioning-bucket-acl-${Date.now()}`;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucket });
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        describe('in bucket w/o versioning cfg :: ', () => {
            beforeEach(async () => {
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
            });

            it('should not return version id for non-version specific ' +
            'put and get ACL', async () => {
                const expectedRes = { versionId: undefined };
                await utils.putAndGetAcl('public-read', undefined, expectedRes);
            });

            it('should not return version id for version specific ' +
            'put and get ACL (version id = "null")', async () => {
                const expectedRes = { versionId: 'null' };
                await utils.putAndGetAcl('public-read', 'null', expectedRes);
            });

            it('should return NoSuchVersion if attempting to put or get acl ' +
            'for non-existing version', async () => {
                const error = { code: 'NoSuchVersion', statusCode: 404 };
                await utils.putAndGetAcl('private', nonExistingId, { error });
            });

            it('should return InvalidArgument if attempting to put/get acl ' +
            'for invalid hex string', async () => {
                const error = { code: 'InvalidArgument', statusCode: 400 };
                await utils.putAndGetAcl('private', invalidId, { error });
            });
        });

        describe('on a version-enabled bucket with non-versioned object :: ',
        () => {
            const versionIds = [];

            beforeEach(async () => {
                const params = { Bucket: bucket, Key: key };
                await s3.send(new PutObjectCommand(params));
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
            });

            afterEach(() => {
                // cleanup versionIds just in case
                versionIds.length = 0;
            });

            describe('before putting new versions :: ', () => {
                it('non-version specific put and get ACL should now ' +
                'return version ID ("null") in response headers', async () => {
                    const expectedRes = { versionId: 'null' };
                    await utils.putAndGetAcl('public-read', undefined, expectedRes);
                });
            });

            describe('after putting new versions :: ', () => {
                beforeEach(async () => {
                    const params = { Bucket: bucket, Key: key };
                    for (let i = 0; i < counter; i++) {
                        const data = await s3.send(new PutObjectCommand(params));
                        _Utils.assertNoError(null, `putting version #${i}`);
                        versionIds.push(data.VersionId);
                    }
                });

                _testBehaviorVersioningEnabledOrSuspended(utils, versionIds);
            });
        });

        describe('on a version-enabled bucket - version non-specified :: ',
        () => {
            let versionId;
            beforeEach(async () => {
                const params = { Bucket: bucket, Key: key };
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
                const data = await s3.send(new PutObjectCommand(params));
                versionId = data.VersionId;
            });

            it('should not create version putting ACL on a' +
            'version-enabled bucket where no version id is specified',
            async () => {
                const params = { Bucket: bucket, Key: key, ACL: 'public-read' };
                await utils.putObjectAcl(params);
                await checkOneVersion(s3, bucket, versionId);
            });
        });

        describe('on version-suspended bucket with non-versioned object :: ',
        () => {
            const versionIds = [];

            beforeEach(async () => {
                const params = { Bucket: bucket, Key: key };
                await s3.send(new PutObjectCommand(params));
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));
            });

            afterEach(() => {
                // cleanup versionIds just in case
                versionIds.length = 0;
            });

            describe('before putting new versions :: ', () => {
                it('non-version specific put and get ACL should still ' +
                'return version ID ("null") in response headers', async () => {
                    const expectedRes = { versionId: 'null' };
                    await utils.putAndGetAcl('public-read', undefined, expectedRes);
                });
            });

            describe('after putting new versions :: ', () => {
                beforeEach(async () => {
                    const params = { Bucket: bucket, Key: key };
                    await s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }));
                    
                    for (let i = 0; i < counter; i++) {
                        const data = await s3.send(new PutObjectCommand(params));
                        _Utils.assertNoError(null, `putting version #${i}`);
                        versionIds.push(data.VersionId);
                    }
                    
                    await s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }));
                });

                _testBehaviorVersioningEnabledOrSuspended(utils, versionIds);
            });
        });
    });
});
