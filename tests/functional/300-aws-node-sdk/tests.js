'use strict'; // eslint-disable-line strict

const assert = require('assert');

const awssdk = require('aws-sdk');

const config = awssdk.config;
const S3 = awssdk.S3;

describe('aws-node-sdk test suite as registered user', function testSuite() {
    let s3;

    before(function setup(done) {
        config.accessKeyId = 'accessKey1';
        config.secretAccessKey = 'verySecretKey1';
        if (process.env.IP !== undefined) {
            config.endpoint = `http://${process.env.IP}:8000`;
        } else {
            config.endpoint = 'http://localhost:8000';
        }
        config.sslEnabled = false;
        config.s3ForcePathStyle = true;
        config.apiVersions = { s3: '2006-03-01' };
        config.logger = process.stdout;
        s3 = new S3();
        done();
    });

    it('should do an empty listing', function emptyListing(done) {
        s3.listBuckets((err, data) => {
            if (err) {
                done(new Error(`error listing buckets: ${err}`));
            } else {
                assert.strictEqual(data.Buckets.length, 0);
                assert(data.Owner, 'No owner Info sent back');
                assert(data.Owner.ID, 'Owner ID not sent back');
                assert(data.Owner.DisplayName, 'DisplayName not sent back');
                const owner = Object.keys(data.Owner);
                assert.strictEqual(owner.length, 2, 'Too much fields in owner');
                done();
            }
        });
    });
});
