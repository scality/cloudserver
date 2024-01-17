const assert = require('assert');

const { makeS3Request } = require('../utils/makeRequest');
const { randomUUID } = require('crypto');

const authCredentials = {
    accessKey: process.env.AWS_ON_AIR ? 'awsAK' : 'accessKey1',
    secretKey: process.env.AWS_ON_AIR ? 'awsSK' : 'verySecretKey1',
};

const bucket = `rawnodelifecyclebucket-${randomUUID()}`;

function makeLifeCycleXML(date) {
    return `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Rule>
        <Expiration>
            <Date>${date}</Date>
        </Expiration>
        <ID>my-id</ID>
        <Filter />
        <Status>Enabled</Status>
    </Rule>
</LifecycleConfiguration>`;
}

describe('api tests', () => {
    before(done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        makeS3Request({
            method: 'DELETE',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should accept a lifecycle policy with a date at midnight', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T00:00:00Z'),
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            return done();
        });
    });

    it('should accept a lifecycle policy with a date at midnight', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T00:00:00'),
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            return done();
        });
    });

    it('should accept a lifecycle policy with a date at midnight', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T06:00:00+06:00'),
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 200);
            return done();
        });
    });

    it('should reject a lifecycle policy with a date not at midnight', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T12:34:56Z'),
        }, err => {
            assert.strictEqual(err.code, 'InvalidArgument');
            assert.strictEqual(err.statusCode, 400);
            return done();
        });
    });

    it('should reject a lifecycle policy with an illegal date', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T00:00:00+34:00'),
        }, err => {
            // This value is catched by AWS during XML parsing
            assert(err.code === 'InvalidArgument' || err.code === 'MalformedXML');
            assert.strictEqual(err.statusCode, 400);
            return done();
        });
    });

    it('should reject a lifecycle policy with a date not at midnight', done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
            queryObj: { lifecycle: '' },
            requestBody: makeLifeCycleXML('2024-01-08T00:00:00.123Z'),
        }, err => {
            assert.strictEqual(err.code, 'InvalidArgument');
            assert.strictEqual(err.statusCode, 400);
            return done();
        });
    });
});
