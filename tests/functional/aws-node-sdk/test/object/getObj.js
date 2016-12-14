import assert from 'assert';
import Promise from 'bluebird';

import { testing } from 'arsenal';
import BucketUtility from '../../lib/utility/bucket-util';


describe('Bucket GET (object listing)', () => {
    const bucketName = 'test-get-bucket';
    const validPrefix = ['/validPrefix/ThatIsPresent/InTheTest/',
    '|validPrefix|ThatIsPresent|InTheTest|'];
    const validMaxKeys = [undefined, 1, 42, 1000, 1001];

    const params = {
        auth: [{}, { signatureVersion: 'v4' }],
        Bucket: [undefined, 'invalid-bucket-name', bucketName, null],
        Delimiter: [undefined, '/', '', '|', null],
        Prefix: [undefined, '/validPrefix/ThatIsNot/InTheSet',
        'InvalidPrefix', '/ThatIsPresent/validPrefix/InTheTest']
        .concat(validPrefix),
        MaxKeys: [undefined, -1, 'string', 0].concat(validMaxKeys),
        EncodingType: [undefined, 'url', null],
    };


    before(done => {
        const bucketUtil = new BucketUtility('default');

        const generateDataSet = () => {
            const Bucket = bucketName;
            /**
            * String.fromCharCode(1) is for testing url encoding
            */
            const UrlTest = [String.fromCharCode(1), '_url_test'].join('');
            const s3 = bucketUtil.s3;
            /**
            * Put 1200 objects with / as delimiter
            */
            for (let i = 0; i !== 1200; ++i) {
                const Prefix = validPrefix[0];
                const Key = [Prefix, i.toString(), '_key_', UrlTest].join('');
                const objects = [
                    { Bucket, Key, ACL: 'public-read' },
                ];
                Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync(objects));
            }

            /**
            * Put 250 objects with | as delimiter
            */
            for (let i = 0; i !== 250; ++i) {
                const Prefix = validPrefix[1];
                const Key = [Prefix, i.toString(), '_key_', UrlTest].join();
                const objects = [
                    { Bucket, Key, ACL: 'public-read' },
                ];
                Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync(objects));
            }
        };

        /**
        * Create bucket if is not created
        * If the bucket is already create, we must empty it
        */
        bucketUtil.createOne(bucketName)
        .then(() => {
            generateDataSet();
            done();
        }).catch(() => {
            bucketUtil.empty(bucketName).then(() => {
                generateDataSet();
                done();
            });
        });
    });

    after(done => {
        const bucketUtil = new BucketUtility('default');

        bucketUtil.empty(bucketName).then(() => {
            done();
            bucketUtil.deleteOne(bucketName);
        }).catch(done);
    });

    const matrix = new testing.matrix.TestMatrix(params);

    matrix.generate(['auth'], matrix => {
        const bucketUtil = new BucketUtility('default', matrix.params.auth);

        matrix.generate(['Delimiter', 'Prefix', 'MaxKeys', 'EncodingType',
        'Bucket'], (matrix, done) => {
            /**
            * usual test
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;
            bucketUtil.s3.listObjects(awsParams, (err, data) => {
                const MaxKeys = awsParams.MaxKeys;
                const maxNumberOfKeys = Math.min(1200, MaxKeys);

                const prefix = awsParams.Prefix;
                const delimiter = awsParams.Delimiter;

                const isPrefixMatch = (delimiter && prefix)
                ? prefix.indexOf(delimiter) !== -1
                : false;
                const isGoodPrefix = validPrefix.indexOf(prefix) !== -1
                || prefix === undefined;
                if (isGoodPrefix === false) {
                    assert.equal(err, null);
                    assert.equal(data.Contents.length === 0
                        || data.Contents === undefined, true);
                } else if (awsParams.MaxKeys !== undefined
                    && isGoodPrefix && isPrefixMatch) {
                    assert.equal(err, null);

                    const NumberOfData = data.Contents.length;
                    assert.equal(NumberOfData <= maxNumberOfKeys, true);
                    assert.equal(NumberOfData === 0 || NumberOfData > 0, true);
                    /**
                    * No need to implement the other test if
                    * EncodingType is not null because we want to see
                    * they didn't encode string.
                    **/
                    if (awsParams.EncodingType === 'url'
			|| NumberOfData === 0) {
                        done();
                        return;
                    }

                    const specialCharacter = String.fromCharCode(1);
                    const idxSpecialCharacter = data.Contents[0]
                    .Key.indexOf(specialCharacter);
                    assert.equal(idxSpecialCharacter !== -1, true);
                }
                done();
            });
        }, 'should list objects')
        .if({ Bucket: [undefined, 'invalid-bucket-name', null] },
        (matrix, done) => {
            /**
            * Invalid bucket name test
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;
            bucketUtil.s3.listObjects(awsParams, (err, data) => {
                assert.equal(err !== null, true);
                assert.equal(data, null);
                done();
            });
        }, 'should have error on invalid bucket')
        .if({ MaxKeys: [-1, 'string'] }, (matrix, done) => {
            /**
            * Invalid max key test
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;
            bucketUtil.s3.listObjects(awsParams, err => {
                assert.equal(err !== null, true);
                done();
            });
        }, 'should have error on invalid max keys')
        .if({ Bucket: [bucketName], EncodingType: ['url'],
        MaxKeys: validMaxKeys, Delimiter: ['/'],
        Prefix: ['/validPrefix/ThatIsPresent/InTheTest/'] },
        (matrix, done) => {
            /**
            * Url encode test
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;
            bucketUtil.s3.listObjects(awsParams, (err, data) => {
                assert.equal(err, null);
                assert.equal(data.Contents !== null, true);
                if (data.Contents !== null) {
                    assert.equal(data.Contents[0].Key.indexOf('%01') !== -1,
                    true);
                }
                done();
            });
        }, 'should have url encoding on object')
        .if({ Bucket: [bucketName], Delimiter: ['|'],
        MaxKeys: validMaxKeys,
        Prefix: ['|validPrefix|ThatIsPresent|InTheTest'] },
        (matrix, done) => {
            /**
            * Specific prefix test
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;

            const maxNumberOfKeys = Math.min(awsParams.MaxKeys, 250);
            bucketUtil.s3.listObjects(awsParams, (err, data) => {
                assert.equal(err, null);
                assert.equal(data.Contents.length <= maxNumberOfKeys, true);
                done();
            });
        }, 'should have object with specific prefix')
        .if({ Bucket: [bucketName], Delimiter: ['/'],
        Prefix: ['/validPrefix/ThatIsNot/InTheSet', 'InvalidPrefix',
        '/ThatIsPresent/validPrefix/InTheTest', null],
        MaxKeys: validMaxKeys },
        (matrix, done) => {
            /**
            * Invalid prefix
            */
            const awsParams = Object.assign({}, matrix.params);
            delete awsParams.auth;
            bucketUtil.s3.listObjects(awsParams, (err, data) => {
                const dataIsNull = data.Contents === null;
                const dataIsEmpty = dataIsNull || data.Contents.length === 0;
                assert.equal(err, null);
                assert.equal(dataIsEmpty, true);
                done();
            });
        }, 'should have no data on invalid prefix');
    }).execute();
});
