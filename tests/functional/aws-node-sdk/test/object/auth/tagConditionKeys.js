const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { makeTagQuery, updateRequestContexts } =
    require('../../../../../../lib/api/apiUtils/authorization/tagConditionKeys');
const { DummyRequestLogger, TaggingConfigTester, createRequestContext } = require('../../../../../unit/helpers');

const taggingUtil = new TaggingConfigTester();
const log = new DummyRequestLogger();
const bucket = 'bucket2testconditionkeys';
const object = 'object2testconditionkeys';
const objPutTaggingReq = taggingUtil
.createObjectTaggingRequest('PUT', bucket, object);
const requestContexts = [createRequestContext('objectPutTagging', objPutTaggingReq)];

describe('Tag condition keys updateRequestContext', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketPromise({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            }));

        it('should update request contexts with request tags and existing object tags', done => {
            const tagsToExist = 'oneKey=oneValue&twoKey=twoValue';
            const params = { Bucket: bucket, Key: object, Tagging: tagsToExist };
            s3.putObject(params, err => {
                assert.ifError(err);
                updateRequestContexts(objPutTaggingReq, requestContexts, log, (err, newRequestContexts) => {
                    assert.ifError(err);
                    assert(newRequestContexts[0].getNeedTagEval());
                    assert.strictEqual(newRequestContexts[0].getExistingObjTag(), tagsToExist);
                    assert.strictEqual(newRequestContexts[0].getRequestObjTags(), makeTagQuery(taggingUtil.getTags()));
                    done();
                });
            });
        });
    });
});
