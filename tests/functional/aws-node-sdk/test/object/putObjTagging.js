import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';
import { taggingTests } from '../../lib/utility/tagging';

function generateMultipleTagConfig(number) {
    const tags = [];
    for (let i = 0; i < number; i++) {
        tags.push({ Key: `myKey${i}`, Value: `myValue${i}` });
    }
    return {
        TagSet: tags,
    };
}
function generateTaggingConfig(key, value) {
    return {
        TagSet: [
            {
                Key: key,
                Value: value,
            },
        ],
    };
}

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}

describe('PUT object taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => s3.createBucket({ Bucket: bucketName }, err => {
            if (err) {
                return done(err);
            }
            return s3.putObject({ Bucket: bucketName, Key: objectName }, done);
        }));

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucketName);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, done => {
                const taggingConfig = generateTaggingConfig(taggingTest.tag.key,
                  taggingTest.tag.value);
                s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
                  Tagging: taggingConfig }, (err, data) => {
                    if (taggingTest.error) {
                        _checkError(err, taggingTest.error, 400);
                    } else {
                        assert.ifError(err, `Found unexpected err ${err}`);
                        assert.strictEqual(Object.keys(data).length, 0);
                    }
                    done();
                });
            });
        });

        it('should return BadRequest if putting more that 10 tags', done => {
            const taggingConfig = generateMultipleTagConfig(11);
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
              Tagging: taggingConfig }, err => {
                _checkError(err, 'BadRequest', 400);
                done();
            });
        });

        it('should return InvalidTag if using the same key twice', done => {
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
              Tagging: { TagSet: [
                  {
                      Key: 'key1',
                      Value: 'value1',
                  },
                  {
                      Key: 'key1',
                      Value: 'value2',
                  },
              ] },
          }, err => {
                _checkError(err, 'InvalidTag', 400);
                done();
            });
        });

        it('should be able to put an empty Tag set', done => {
            s3.putObjectTagging({ Bucket: bucketName, Key: objectName,
              Tagging: { TagSet: [] },
          }, (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(Object.keys(data).length, 0);
                done();
            });
        });

        it('should return NoSuchKey put tag to a non-existing object', done => {
            s3.putObjectTagging({
                Bucket: bucketName,
                Key: 'nonexisting',
                Tagging: { TagSet: [
                    {
                        Key: 'key1',
                        Value: 'value1',
                    }] },
            }, err => {
                _checkError(err, 'NoSuchKey', 404);
                done();
            });
        });
    });
});
