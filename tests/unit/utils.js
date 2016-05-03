import assert from 'assert';

import utils from '../../lib/utils';


describe('utils.getBucketNameFromHost', () => {
    it('should extract valid buckets for one endpoint', () => {
        [
            'b', 'mybucket',
            'buck-et', '-buck-et', 'buck-et-',
            'buck_et', '_buck_et', 'buck_et_',
            'buck.et', 'bu.ck.et', 'bu.ck-et',
        ].forEach(bucket => {
            const headers = {
                host: `${bucket}.s3.eu-west-1.amazonaws.com`,
            };
            const result = utils.getBucketNameFromHost({ headers });
            assert.strictEqual(result, bucket);
        });
    });

    it('should also accept website endpoints', () => {
        [
            'in-french.bucket.is-seau.s3-website-eu-west-1.amazonaws.com',
            'in-french.bucket.is-seau.s3-website-us-east-1.amazonaws.com',
            'in-french.bucket.is-seau.s3-website-ap-southeast-2.amazonaws.com',
            'in-french.bucket.is-seau.s3-website-eu-central-1.amazonaws.com',
            'in-french.bucket.is-seau.s3-website-ap-northeast-1.amazonaws.com',
        ].forEach(host => {
            const headers = { host };
            const result = utils.getBucketNameFromHost({ headers });
            assert.strictEqual(result, 'in-french.bucket.is-seau');
        });
    });

    it('should return undefined when non dns-style', () => {
        [
            's3.amazonaws.com',
            's3.eu.central-1.amazonaws.com',
            's3.eu-west-1.amazonaws.com',
            's3-external-1.amazonaws.com',
            's3.us-east-1.amazonaws.com',
            's3-us-gov-west-1.amazonaws.com',
            's3-fips-us-gov-west-1.amazonaws.com',
        ].forEach(host => {
            const headers = { host };
            const result = utils.getBucketNameFromHost({ headers });
            assert.strictEqual(result, undefined);
        });
    });

    it('should return undefined when IP addresses', () => {
        [
            '127.0.0.1',
            '8.8.8.8',
        ].forEach(host => {
            const headers = { host };
            const result = utils.getBucketNameFromHost({ headers });
            assert.strictEqual(result, undefined);
        });
    });

    it('should throw when bad request', () => {
        [
            {},
            { host: '' },
            { host: 'not/a/valid/endpoint' },
            { host: 'this.domain.is.not.in.config' },
        ].forEach(headers => {
            assert.throws(() => {
                utils.getBucketNameFromHost({ headers });
            });
        });
    });
});

describe('utils.getAllRegions', () => {
    it('should return official AWS regions', () => {
        const allRegions = utils.getAllRegions();

        assert(allRegions.indexOf('ap-northeast-1') >= 0);
        assert(allRegions.indexOf('ap-southeast-1') >= 0);
        assert(allRegions.indexOf('ap-southeast-2') >= 0);
        assert(allRegions.indexOf('eu-central-1') >= 0);
        assert(allRegions.indexOf('eu-west-1') >= 0);
        assert(allRegions.indexOf('sa-east-1') >= 0);
        assert(allRegions.indexOf('us-west-1') >= 0);
        assert(allRegions.indexOf('us-west-2') >= 0);
        assert(allRegions.indexOf('us-east-1') >= 0);
        assert(allRegions.indexOf('us-gov-west-1') >= 0);
    });

    it('should return regions from config', () => {
        const allRegions = utils.getAllRegions();

        assert(allRegions.indexOf('localregion') >= 0);
    });
});

describe('utils.getAllEndpoints', () => {
    it('should return endpoints from config', () => {
        const allEndpoints = utils.getAllEndpoints();

        assert(allEndpoints.indexOf('s3-us-west-2.amazonaws.com') >= 0);
        assert(allEndpoints.indexOf('s3.amazonaws.com') >= 0);
        assert(allEndpoints.indexOf('s3-external-1.amazonaws.com') >= 0);
        assert(allEndpoints.indexOf('s3.us-east-1.amazonaws.com') >= 0);
        assert(allEndpoints.indexOf('localhost') >= 0);
    });
});

describe('utils.isValidBucketName', () => {
    it('should return false if bucketname is fewer than ' +
        '3 characters long', () => {
        const result = utils.isValidBucketName('no');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is greater than ' +
        '63 characters long', () => {
        const longString = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
        const result = utils.isValidBucketName(longString);
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname contains capital letters', () => {
        const result = utils.isValidBucketName('noSHOUTING');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is an IP address', () => {
        const result = utils.isValidBucketName('172.16.254.1');
        assert.strictEqual(result, false);
    });

    it('should return false if bucketname is not DNS compatible', () => {
        const result = utils.isValidBucketName('*notvalid*');
        assert.strictEqual(result, false);
    });

    it('should return true if bucketname does not break rules', () => {
        const result = utils.isValidBucketName('okay');
        assert.strictEqual(result, true);
    });
});

const bucketName = 'bucketname';
const objName = 'testObject';

describe('utils.normalizeRequest', () => {
    it('should parse bucket name from path', () => {
        const request = {
            url: `/${bucketName}`,
            headers: { host: 's3.amazonaws.com' },
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket name from host', () => {
        const request = {
            url: '/',
            headers: { host: `${bucketName}.s3.amazonaws.com` },
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket and object name from path', () => {
        const request = {
            url: `/${bucketName}/${objName}`,
            headers: { host: 's3.amazonaws.com' },
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.objectKey, objName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });

    it('should parse bucket name from host ' +
        'and object name from path', () => {
        const request = {
            url: `/${objName}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
        };
        const result = utils.normalizeRequest(request);
        assert.strictEqual(result.bucketName, bucketName);
        assert.strictEqual(result.objectKey, objName);
        assert.strictEqual(result.parsedHost, 's3.amazonaws.com');
    });
});
