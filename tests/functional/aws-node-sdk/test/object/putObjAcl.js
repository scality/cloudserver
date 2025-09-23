const assert = require('assert');
const {
    PutObjectCommand,
    PutObjectAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const constants = require('../../../../../constants');

const notOwnerCanonicalID = '79a59df900b949e55d96a1e698fba' +
    'cedfd6e09d98eacf8f8d5218e7cd47ef2bf';
const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

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

describe('PUT Object ACL', () => {
    withV4(sigCfg => {
        let bucketName;
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const Key = 'aclTest';

        before(async () => {
            bucketName = await bucketUtil.createRandom(1);
        });

        afterEach(async () => {
            process.stdout.write('emptying bucket');
            await bucketUtil.empty(bucketName);
        });

        after(async () => {
            process.stdout.write('deleting bucket');
            await bucketUtil.deleteOne(bucketName);
        });

        it('should put object ACLs', async () => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
                { Bucket, Key },
            ];
            for (const param of objects) {
                await s3.send(new PutObjectCommand(param));
            }
            const data = await s3.send(new PutObjectAclCommand({ 
                Bucket, 
                Key, 
                ACL: 'public-read' 
            }));
            assert(data);
        });        

        it('should return NoSuchKey if try to put object ACLs ' +
            'for nonexistent object', async () => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;

            try {
                await s3.send(new PutObjectAclCommand({
                    Bucket,
                    Key,
                    ACL: 'public-read' 
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                assert(err);
                assert.strictEqual(err.$metadata.httpStatusCode, 404);
                assert.strictEqual(err.name, 'NoSuchKey');
            }
        });

        describe('on an object', () => {
            before(async () => {
                await s3.send(new PutObjectCommand({ Bucket: bucketName, Key }));
            });
            
            after(async () => {
                process.stdout.write('deleting bucket');
                await bucketUtil.empty(bucketName);
            });
            
            // The supplied canonical ID is not associated with a real AWS
            // account, so AWS_ON_AIR will raise a 400 InvalidArgument
            itSkipIfAWS('should return AccessDenied if try to change owner ' +
                'ID in ACL request body', async () => {
                const acp = new _AccessControlPolicy(
                    { ownerID: notOwnerCanonicalID });
                acp.addGrantee('Group', constants.publicId, 'READ');
                const putAclParams = {
                    Bucket: bucketName,
                    Key,
                    AccessControlPolicy: acp,
                };
                
                try {
                    await s3.send(new PutObjectAclCommand(putAclParams));
                    throw new Error('Expected AccessDenied error');
                } catch (err) {
                    assert(err);
                    assert.strictEqual(err.$metadata.httpStatusCode, 403);
                    assert.strictEqual(err.name, 'AccessDenied');
                }
            });
        });
    });
});
