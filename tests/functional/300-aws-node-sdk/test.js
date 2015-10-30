import { expect } from 'chai';
import { config, S3 } from 'aws-sdk';

suite('aws-node-sdk', function () {
    let s3;
    setup(function (done) {
        config.accessKeyId = 'accessKey1';
        config.secretAccessKey = 'verySecretKey1';
        config.endpoint = 'http://' + process.env.IP + ':8000';
        config.sslEnabled = false;
        config.logger = process.stdout;
        console.log('testing on ' + process.env.IP);
        s3 = new S3();
        done();
    });
    test('empty listing', function (done) {
        s3.listBuckets( function (err, data) {
            if (err) {
                throw err;
            } else {
                expect(data.Buckets).to.be.empty;
                done();
            }
        });
    });
});
