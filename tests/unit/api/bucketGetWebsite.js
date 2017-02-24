import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutWebsite from '../../../lib/api/bucketPutWebsite';
import bucketGetWebsite from '../../../lib/api/bucketGetWebsite';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
from '../helpers';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetWebsiteTestBucket';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _makeWebsiteRequest(xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?website',
        query: { website: '' },
    };

    if (xml) {
        request.post = xml;
    }
    return request;
}
const testGetWebsiteRequest = _makeWebsiteRequest();

function _comparePutGetXml(sampleXml, done) {
    const fullXml = '<?xml version="1.0" encoding="UTF-8" ' +
    'standalone="yes"?><WebsiteConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `${sampleXml}</WebsiteConfiguration>`;
    const testPutWebsiteRequest = _makeWebsiteRequest(fullXml);
    bucketPutWebsite(authInfo, testPutWebsiteRequest, log, err => {
        if (err) {
            process.stdout.write(`Err putting website config ${err}`);
            return done(err);
        }
        return bucketGetWebsite(authInfo, testGetWebsiteRequest, log,
        (err, res) => {
            assert.strictEqual(err, null, `Unexpected err ${err}`);
            assert.strictEqual(res, fullXml);
            done();
        });
    });
}

describe('getBucketWebsite API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should return same IndexDocument XML as uploaded', done => {
        const sampleXml =
            '<IndexDocument><Suffix>index.html</Suffix></IndexDocument>';
        _comparePutGetXml(sampleXml, done);
    });
    it('should return same ErrorDocument XML as uploaded', done => {
        const sampleXml =
            '<IndexDocument><Suffix>index.html</Suffix></IndexDocument>' +
            '<ErrorDocument><Key>error.html</Key></ErrorDocument>';
        _comparePutGetXml(sampleXml, done);
    });
    it('should return same RedirectAllRequestsTo as uploaded', done => {
        const sampleXml =
            '<RedirectAllRequestsTo>' +
            '<HostName>test</HostName>' +
            '<Protocol>http</Protocol>' +
            '</RedirectAllRequestsTo>';
        _comparePutGetXml(sampleXml, done);
    });
    it('should return same RoutingRules as uploaded', done => {
        const sampleXml =
            '<IndexDocument><Suffix>index.html</Suffix></IndexDocument>' +
            '<RoutingRules><RoutingRule>' +
            '<Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>' +
            '<Redirect><HostName>test</HostName></Redirect>' +
            '</RoutingRule><RoutingRule>' +
            '<Condition>' +
            '<HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>' +
            '</Condition>' +
            '<Redirect><HttpRedirectCode>303</HttpRedirectCode></Redirect>' +
            '</RoutingRule></RoutingRules>';
        _comparePutGetXml(sampleXml, done);
    });
});
