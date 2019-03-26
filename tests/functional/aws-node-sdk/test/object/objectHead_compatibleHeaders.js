const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'objectheadtestheaders';
const objectName = 'someObject';

describe('HEAD object, compatibility headers [Cache-Control, ' +
  'Content-Disposition, Content-Encoding, Expires]', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const cacheControl = 'max-age=86400';
        const contentDisposition = 'attachment; filename="fname.ext";';
        const contentEncoding = 'gzip,aws-chunked';
        // AWS Node SDK requires Date object, ISO-8601 string, or
        // a UNIX timestamp for Expires header
        const expires = new Date();

        beforeAll(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return bucketUtil.empty(bucketName).then(() =>
                bucketUtil.deleteOne(bucketName)
            )
            .catch(err => {
                if (err.code !== 'NoSuchBucket') {
                    process.stdout.write(`${err}\n`);
                    throw err;
                }
            })
            .then(() => bucketUtil.createOne(bucketName))
            .then(() => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                    CacheControl: cacheControl,
                    ContentDisposition: contentDisposition,
                    ContentEncoding: contentEncoding,
                    Expires: expires,
                };
                return s3.putObjectAsync(params);
            })
            .catch(err => {
                process.stdout.write(`Error with putObject: ${err}\n`);
                throw err;
            });
        });

        afterAll(() => {
            process.stdout.write('deleting bucket');
            return bucketUtil.empty(bucketName).then(() =>
            bucketUtil.deleteOne(bucketName));
        });

        test('should return additional headers if specified in objectPUT ' +
          'request', done => {
            s3.headObject({ Bucket: bucketName, Key: objectName },
              (err, res) => {
                  if (err) {
                      return done(err);
                  }
                  expect(res.CacheControl).toBe(cacheControl);
                  expect(res.ContentDisposition).toBe(contentDisposition);
                  // Should remove V4 streaming value 'aws-chunked'
                  // to be compatible with AWS behavior
                  expect(res.ContentEncoding).toBe('gzip,');
                  expect(res.Expires).toBe(expires.toUTCString());
                  return done();
              });
        });
    });
});
