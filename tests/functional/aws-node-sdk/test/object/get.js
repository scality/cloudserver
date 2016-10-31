import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'buckettestgetobject';
const objectName = 'someObject';
// Specify sample headers to check for in GET response
const cacheControl = 'max-age=86400';
const contentDisposition = 'attachment; filename="fname.ext";';
const contentEncoding = 'aws-chunked,gzip';
// AWS Node SDK requires Date object, ISO-8601 string, or
// a UNIX timestamp for Expires header
const expires = new Date();

describe('GET object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            // Create a bucket to put object to get later
            s3.createBucket({ Bucket: bucketName }, done);
        });

        after(done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });


        it('should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    return done();
                });
            });

        describe('Additional headers: [Cache-Control, Content-Disposition, ' +
        'Content-Encoding, Expires]', () => {
            before(done => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                    CacheControl: cacheControl,
                    ContentDisposition: contentDisposition,
                    ContentEncoding: contentEncoding,
                    Expires: expires,
                };
                s3.putObject(params, err => done(err));
            });
            it('should return additional headers if specified in objectPUT ' +
              'request', done => {
                s3.getObject({ Bucket: bucketName, Key: objectName },
                  (err, res) => {
                      if (err) {
                          return done(err);
                      }
                      assert.strictEqual(res.CacheControl,
                        cacheControl);
                      assert.strictEqual(res.ContentDisposition,
                        contentDisposition);
                      // Should remove V4 streaming value 'aws-chunked'
                      // to be compatible with AWS behavior
                      assert.strictEqual(res.ContentEncoding,
                        'gzip');
                      assert.strictEqual(res.Expires,
                          expires.toGMTString());
                      return done();
                  });
            });
        });
    });
});
