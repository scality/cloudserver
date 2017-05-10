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
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with website config obj', done => {
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
                assert.strictEqual(bucketWebsiteConfig._indexDocument,
                    config.IndexDocument.Suffix);
                assert.strictEqual(bucketWebsiteConfig._errorDocument,
                    config.ErrorDocument.Key);
                assert.strictEqual(bucketWebsiteConfig._routingRules[0]
                    ._condition.keyPrefixEquals,
                    config.RoutingRules[0].Condition.KeyPrefixEquals);
                assert.strictEqual(bucketWebsiteConfig._routingRules[0]
                    ._redirect.replaceKeyPrefixWith,
                    config.RoutingRules[0].Redirect.ReplaceKeyPrefixWith);
                return done();
            });
        });
    });

    describe('helper functions', () => {
        it('xmlContainsElem should return true if xml contains ' +
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
                assert.strictEqual(containsRes, true);
                return done();
            });
        });
        it('xmlContainsElem should return false if xml does not contain ' +
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
                assert.strictEqual(containsRes, false);
                return done();
            });
        });
        it('xmlContainsElem should return true if parent contains list of ' +
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
                assert.strictEqual(containsRes, true);
                return done();
            });
        });
        it('xmlContainsElem should return true if parent contains at least ' +
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
                assert.strictEqual(containsRes, true);
                return done();
            });
        });
        it('xmlContainsElem should return false if parent contains only one ' +
        'of multiple elements specified and checkForAll specified in options',
        done => {
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
                assert.strictEqual(containsRes, false);
                return done();
            });
        });
        it('xmlContainsElem should return true if parent contains all ' +
        'of multiple elements specified and checkForAll specified in options',
        done => {
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
                assert.strictEqual(containsRes, true);
                return done();
            });
        });
    });
});
