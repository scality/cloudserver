import { errors } from 'arsenal';
import assert from 'assert';
import lolex from 'lolex';

import { createAlteredRequest } from '../../helpers';
import headerAuthCheck from
    '../../../../lib/auth/v4/headerAuthCheck';
import { DummyRequestLogger, makeAuthInfo } from '../../helpers';

const log = new DummyRequestLogger();

const method = 'PUT';
const path = '/mybucket';
const xAMZcontentSha256 = '771df8abbecb2265e9724e5dc4510dcc160' +
    '60c0513ae669baf35b255d465b63f';
const host = 'localhost:8000';
const xAMZdate = '20160208T201405Z';
const authorization = 'AWS4-HMAC-SHA256 Credential=accessKey1/20160208' +
    '/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;' +
    'x-amz-date, Signature=abed924c06abf8772c670064d22eacd6ccb85c06befa15f' +
    '4a789b0bae19307bc';
const headers = {
    host,
    authorization,
    'x-amz-date': xAMZdate,
    'x-amz-content-sha256': xAMZcontentSha256,
};
const request = {
    method,
    path,
    headers,
    query: {},
};
const createdAuthInfo = makeAuthInfo('accessKey1');

describe('v4 headerAuthCheck', () => {
    it('should return error if undefined authorization header', done => {
        const alteredRequest = createAlteredRequest({
            authorization: undefined }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if undefined sha256 header', done => {
        const alteredRequest = createAlteredRequest({
            'x-amz-content-sha256': undefined }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if missing credentials', done => {
        const alteredRequest = createAlteredRequest({
            authorization: 'AWS4-HMAC-SHA256 SignedHeaders=host;' +
                'x-amz-content-sha256;x-amz-date, Signature=abed9' +
                '24c06abf8772c670064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if missing SignedHeaders', done => {
        // 'Sigheaders' instead of SignedHeaders in authorization
        const alteredRequest = createAlteredRequest({
            authorization: 'AWS4-HMAC-SHA256 Credential=accessKey1' +
                '/20160208/us-east-1/s3/aws4_request, ' +
                'Sigheaders=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c6' +
                '70064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if missing Signature', done => {
        // Sig instead of 'Signature' in authorization
        const alteredRequest = createAlteredRequest({
            authorization: 'AWS4-HMAC-SHA256 Credential=accessKey1' +
                '/20160208/us-east-1/s3/aws4_request, ' +
                'SignedHeaders=host;x-amz-content-sha256;' +
                'x-amz-date, Sig=abed924c06abf8772c6' +
                '70064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if missing timestamp', done => {
        const alteredRequest = createAlteredRequest({
            'x-amz-date': undefined }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });

    it('should return error if scope date does not ' +
        'match timestamp date', done => {
        // Different timestamp (2015 instead of 2016)
        const alteredRequest = createAlteredRequest({
            'x-amz-date': '20150208T201405Z' }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    it('should return error if timestamp from x-amz-date header' +
        'is in the future', done => {
        // Different date (2095 instead of 2016)
        const alteredRequest = createAlteredRequest({
            'x-amz-date': '20950208T201405Z',
            'authorization': 'AWS4-HMAC-SHA256 Credential' +
                '=accessKey1/20950208/us-east-1/s3/aws4_request, ' +
                'SignedHeaders=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c67' +
                '0064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' }, 'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.RequestTimeTooSkewed);
            done();
        });
    });

    it('should return error if timestamp from date header' +
        ' is in the future (and there is no x-amz-date header)', done => {
        const alteredRequest = createAlteredRequest({
            date: 'Tue, 08 Feb 2095 20:14:05 GMT',
            authorization: 'AWS4-HMAC-SHA256 Credential' +
                '=accessKey1/20950208/us-east-1/s3/aws4_request, ' +
                'SignedHeaders=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c67' +
                '0064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' },
            'headers', request, headers);
        delete alteredRequest.headers['x-amz-date'];
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.RequestTimeTooSkewed);
            done();
        });
    });

    it('should return error if timestamp from x-amz-date header' +
        'is too old', done => {
        // Different scope date and x-amz-date (2015 instead of 2016)
        const alteredRequest = createAlteredRequest({
            'x-amz-date': '20150208T201405Z',
            'authorization': 'AWS4-HMAC-SHA256 Credential' +
                '=accessKey1/20150208/us-east-1/s3/aws4_request, ' +
                'SignedHeaders=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c67' +
                '0064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' },
            'headers', request, headers);
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.RequestTimeTooSkewed);
            done();
        });
    });

    it('should return error if timestamp from date header' +
        'is too old (and there is no x-amz-date header)', done => {
        // Different scope date (2015 instead of 2016) and date in 2015
        const alteredRequest = createAlteredRequest({
            date: 'Sun, 08 Feb 2015 20:14:05 GMT',
            authorization: 'AWS4-HMAC-SHA256 Credential' +
                '=accessKey1/20150208/us-east-1/s3/aws4_request, ' +
                'SignedHeaders=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c67' +
                '0064d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' },
            'headers', request, headers);
        delete alteredRequest.headers['x-amz-date'];
        headerAuthCheck(alteredRequest, log, err => {
            assert.deepStrictEqual(err, errors.RequestTimeTooSkewed);
            done();
        });
    });

    it('should not return error due to unknown region', done => {
        // Returning an error causes an issue for certain clients.
        const alteredRequest = createAlteredRequest({
            authorization: 'AWS4-HMAC-SHA256 Credential=accessKey1/20160208' +
                '/noSuchRegion/s3/aws4_request, SignedHeaders' +
                '=host;x-amz-content-sha256;' +
                'x-amz-date, Signature=90235ffa4277d688072e16fa7a7560044f4f' +
                '8e43e369f48ea6d3a5f1fe518e14',
        }, 'headers', request, headers);
        const clock = lolex.install(1454962445000);
        headerAuthCheck(alteredRequest, log, err => {
            clock.uninstall();
            assert.ifError(err);
            done();
        });
    });

    it('should successfully authenticate', done => {
        // Freezes time so date created within function will be Feb 8, 2016
        const clock = lolex.install(1454962445000);
        headerAuthCheck(request, log, (err, authInfo) => {
            clock.uninstall();
            assert.strictEqual(err, null);
            assert.strictEqual(authInfo.getCanonicalID(),
                createdAuthInfo.getCanonicalID());
            done();
        });
    });

    it('should return error if accessKey does not exist', done => {
        const alteredRequest = createAlteredRequest({
            authorization: 'AWS4-HMAC-SHA256 ' +
                'Credential=nonexistaentkey/20160208' +
                '/us-east-1/s3/aws4_request, SignedHeaders=host;' +
                'x-amz-content-sha256;' +
                'x-amz-date, Signature=abed924c06abf8772c67006' +
                '4d22eacd6ccb85c06befa15f' +
                '4a789b0bae19307bc' }, 'headers', request, headers);
        const clock = lolex.install(1454962445000);
        headerAuthCheck(alteredRequest, log, err => {
            clock.uninstall();
            assert.deepStrictEqual(err, errors.InvalidAccessKeyId);
            done();
        });
    });
});
