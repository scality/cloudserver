const assert = require('assert');
const async = require('async');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const {
    awsLocation,
    enableVersioning,
    putNullVersionsToAws,
    putVersionsToAws,
    getAndAssertResult,
    describeSkipIfNotMultiple,
} = require('../utils');

const someBody = 'testbody';
const bucket = 'buckettestmultiplebackendaclsawsversioning';

class _AccessControlPolicy {
    constructor(params) {
        this.Owner = {};
        this.Owner.ID = params.ownerID;
        if (params.ownerDisplayName) {
            this.Owner.DisplayName = params.ownerDisplayName;
        }
        this.Grants = [];
    }
    addGrantee(type, value, permission, displayName) {
        const grant = {
            Grantee: {
                Type: type,
            },
            Permission: permission,
        };
        if (displayName) {
            grant.Grantee.DisplayName = displayName;
        }
        if (type === 'AmazonCustomerByEmail') {
            grant.Grantee.EmailAddress = value;
        } else if (type === 'CanonicalUser') {
            grant.Grantee.ID = value;
        } else if (type === 'Group') {
            grant.Grantee.URI = value;
        }
        this.Grants.push(grant);
    }
}

const ownerParams = {
    ownerID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
    ownerDisplayName: 'Bart',
};
const testAcp = new _AccessControlPolicy(ownerParams);
testAcp.addGrantee('Group', constants.publicId, 'READ');

function putObjectAcl(s3, key, versionId, acp, cb) {
    s3.putObjectAcl({ Bucket: bucket, Key: key, AccessControlPolicy: acp,
        VersionId: versionId }, err => {
        assert.strictEqual(err, null, 'Expected success ' +
            `putting object acl, got error ${err}`);
        cb();
    });
}

function putObjectAndAcl(s3, key, body, acp, cb) {
    s3.putObject({ Bucket: bucket, Key: key, Body: body },
    (err, putData) => {
        assert.strictEqual(err, null, 'Expected success ' +
            `putting object, got error ${err}`);
        putObjectAcl(s3, key, putData.VersionId, acp, () =>
            cb(null, putData.VersionId));
    });
}

/** putVersionsWithAclToAws - enable versioning and put multiple versions
 * followed by putting object acl
 * @param {AWS.S3} s3 - aws node sdk s3 instance
 * @param {string} key - string
 * @param {(string[]|Buffer[])} data - array of data to put as objects
 * @param {_AccessControlPolicy[]} acps - array of _AccessControlPolicy instance
 * @param {function} cb - callback which expects err and array of version ids
 * @return {undefined} - and call cb
 */
function putVersionsWithAclToAws(s3, key, data, acps, cb) {
    if (data.length !== acps.length) {
        throw new Error('length of data and acp arrays must be the same');
    }
    enableVersioning(s3, bucket, () => {
        async.timesLimit(data.length, 1, (i, next) => {
            putObjectAndAcl(s3, key, data[i], acps[i], next);
        }, (err, results) => {
            assert.strictEqual(err, null, 'Expected success ' +
                `putting versions with acl, got error ${err}`);
            cb(null, results);
        });
    });
}

function getObjectAndAssertAcl(s3, params, cb) {
    const { bucket, key, versionId, body, expectedVersionId, expectedResult }
        = params;
    getAndAssertResult(s3, { bucket, key, versionId, expectedVersionId, body },
        () => {
            s3.getObjectAcl({ Bucket: bucket, Key: key, VersionId: versionId },
                (err, data) => {
                    assert.strictEqual(err, null, 'Expected success ' +
                        `getting object acl, got error ${err}`);
                    assert.deepEqual(data, expectedResult);
                    cb();
                });
        });
}

/** getObjectsAndAssertAcls - enable versioning and put multiple versions
 * followed by putting object acl
 * @param {AWS.S3} s3 - aws node sdk s3 instance
 * @param {string} key - string
 * @param {string[]} versionIds - array of versionIds to use to get objs & acl
 * @param {(string[]|Buffer[])} expectedData - array of data expected from gets
 * @param {_AccessControlPolicy[]} expectedAcps - array of acps expected from
 * get acls
 * @param {function} cb - callback
 * @return {undefined} - and call cb
 */
function getObjectsAndAssertAcls(s3, key, versionIds, expectedData,
    expectedAcps, cb) {
    async.timesLimit(versionIds.length, 1, (i, next) => {
        const versionId = versionIds[i];
        const body = expectedData[i];
        const expectedResult = expectedAcps[i];
        getObjectAndAssertAcl(s3, { bucket, key, versionId, body,
            expectedResult, expectedVersionId: versionId }, next);
    }, err => {
        assert.strictEqual(err, null, 'Expected success ' +
            `getting object acls, got error ${err}`);
        cb();
    });
}

describeSkipIfNotMultiple('AWS backend put/get object acl with versioning',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocation,
                },
            })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });

        it('versioning not configured: should put/get acl successfully when ' +
        'versioning not configured', done => {
            const key = `somekey-${Date.now()}`;
            putObjectAndAcl(s3, key, someBody, testAcp, (err, versionId) => {
                assert.strictEqual(versionId, undefined);
                getObjectAndAssertAcl(s3, { bucket, key, body: someBody,
                    expectedResult: testAcp }, done);
            });
        });

        it('versioning suspended then enabled: should put/get acl on null ' +
        'version successfully even when latest version is not null version',
        done => {
            const key = `somekey-${Date.now()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [undefined],
                    err => next(err)),
                next => putVersionsToAws(s3, bucket, key, [someBody],
                    err => next(err)),
                next => putObjectAcl(s3, key, 'null', testAcp, next),
                next => getObjectAndAssertAcl(s3, { bucket, key, body: '',
                    versionId: 'null', expectedResult: testAcp,
                    expectedVersionId: 'null' }, next),
            ], done);
        });

        it('versioning enabled: should get correct acl using version IDs',
        done => {
            const key = `somekey-${Date.now()}`;
            const acps = ['READ', 'FULL_CONTROL', 'READ_ACP', 'WRITE_ACP']
            .map(perm => {
                const acp = new _AccessControlPolicy(ownerParams);
                acp.addGrantee('Group', constants.publicId, perm);
                return acp;
            });
            const data = [...Array(acps.length).keys()].map(i => i.toString());
            const versionIds = ['null'];
            async.waterfall([
                next => putObjectAndAcl(s3, key, data[0], acps[0],
                    () => next()),
                next => putVersionsWithAclToAws(s3, key, data.slice(1),
                    acps.slice(1), next),
                (ids, next) => {
                    versionIds.push(...ids);
                    next();
                },
                next => getObjectsAndAssertAcls(s3, key, versionIds, data, acps,
                    next),
            ], done);
        });

        it('versioning enabled: should get correct acl when getting ' +
        'without version ID', done => {
            const key = `somekey-${Date.now()}`;
            const acps = ['READ', 'FULL_CONTROL', 'READ_ACP', 'WRITE_ACP']
            .map(perm => {
                const acp = new _AccessControlPolicy(ownerParams);
                acp.addGrantee('Group', constants.publicId, perm);
                return acp;
            });
            const data = [...Array(acps.length).keys()].map(i => i.toString());
            const versionIds = ['null'];
            async.waterfall([
                next => putObjectAndAcl(s3, key, data[0], acps[0],
                    () => next()),
                next => putVersionsWithAclToAws(s3, key, data.slice(1),
                    acps.slice(1), next),
                (ids, next) => {
                    versionIds.push(...ids);
                    next();
                },
                next => getObjectAndAssertAcl(s3, { bucket, key,
                    expectedVersionId: versionIds[3],
                    expectedResult: acps[3], body: data[3] }, next),
            ], done);
        });
    });
});
