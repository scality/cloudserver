const assert = require('assert');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutWebsite = require('../../../lib/api/bucketPutWebsite');
const { xmlContainsElem }
    = require('../../../lib/api/apiUtils/bucket/bucketWebsite');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    WebsiteConfig }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _getPutWebsiteRequest(xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?website',
        query: { website: '' },
    };
    request.post = xml;
    return request;
}

describe('putBucketWebsite API', () => {
    beforeAll(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    test('should update a bucket\'s metadata with website config obj', done => {
        const config = new WebsiteConfig('index.html', 'error.html');
        config.addRoutingRule({ ReplaceKeyPrefixWith: 'documents/' },
        { KeyPrefixEquals: 'docs/' });
        const testBucketPutWebsiteRequest =
            _getPutWebsiteRequest(config.getXml());
        bucketPutWebsite(authInfo, testBucketPutWebsiteRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting website config ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const bucketWebsiteConfig = bucket.getWebsiteConfiguration();
                expect(bucketWebsiteConfig._indexDocument).toBe(config.IndexDocument.Suffix);
                expect(bucketWebsiteConfig._errorDocument).toBe(config.ErrorDocument.Key);
                expect(bucketWebsiteConfig._routingRules[0]
                    ._condition.keyPrefixEquals).toBe(config.RoutingRules[0].Condition.KeyPrefixEquals);
                expect(bucketWebsiteConfig._routingRules[0]
                    ._redirect.replaceKeyPrefixWith).toBe(config.RoutingRules[0].Redirect.ReplaceKeyPrefixWith);
                return done();
            });
        });
    });

    describe('helper functions', () => {
        test('xmlContainsElem should return true if xml contains ' +
        'specified element', done => {
            const xml = '<Toplevel><Parent>' +
            '<Element>value</Element>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    'Element');
                expect(containsRes).toBe(true);
                return done();
            });
        });
        test('xmlContainsElem should return false if xml does not contain ' +
        'specified element', done => {
            const xml = '<Toplevel><Parent>' +
            '<ElementA>value</ElementA>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    'Element');
                expect(containsRes).toBe(false);
                return done();
            });
        });
        test('xmlContainsElem should return true if parent contains list of ' +
        'elements and isList is specified in options', done => {
            const xml = '<Toplevel><Parent>' +
            '<Element>value</Element>' +
            '<Element>value</Element>' +
            '<Element>value</Element>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    'Element', { isList: true });
                expect(containsRes).toBe(true);
                return done();
            });
        });
        test('xmlContainsElem should return true if parent contains at least ' +
        'one of the elements specified, if multiple', done => {
            const xml = '<Toplevel><Parent>' +
            '<ElementB>value</ElementB>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    ['ElementA', 'ElementB']);
                expect(containsRes).toBe(true);
                return done();
            });
        });
        test('xmlContainsElem should return false if parent contains only one ' +
        'of multiple elements specified and checkForAll specified in options', done => {
            const xml = '<Toplevel><Parent>' +
            '<ElementB>value</ElementB>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    ['ElementA', 'ElementB'], { checkForAll: true });
                expect(containsRes).toBe(false);
                return done();
            });
        });
        test('xmlContainsElem should return true if parent contains all ' +
        'of multiple elements specified and checkForAll specified in options', done => {
            const xml = '<Toplevel><Parent>' +
            '<ElementA>value</ElementA>' +
            '<ElementB>value</ElementB>' +
            '</Parent></Toplevel>';
            parseString(xml, (err, result) => {
                if (err) {
                    process.stdout.write(`Unexpected err ${err} parsing xml`);
                    return done(err);
                }
                const containsRes = xmlContainsElem(result.Toplevel.Parent,
                    ['ElementA', 'ElementB'], { checkForAll: true });
                expect(containsRes).toBe(true);
                return done();
            });
        });
    });
});
