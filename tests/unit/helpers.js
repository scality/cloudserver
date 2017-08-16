const crypto = require('crypto');
const assert = require('assert');

const AuthInfo = require('arsenal').auth.AuthInfo;
const constants = require('../../constants');
const { metadata } = require('../../lib/metadata/in_memory/metadata');
const { resetCount, ds } = require('../../lib/data/in_memory/backend');
const DummyRequest = require('./DummyRequest');

const testsRangeOnEmptyFile = [
    { range: 'bytes=0-9', valid: true },
    { range: 'bytes=1-9', valid: true },
    { range: 'bytes=1-999', valid: true },
    { range: 'bytes=0-', valid: true },
    { range: 'bytes=1-', valid: true },
    { range: 'bytes=0-0', valid: true },
    { range: 'bytes=00-0000', valid: true },
    { range: 'bytes=1-1', valid: true },
    { range: 'bytes=-0', valid: true },
    { range: 'bytes=-000', valid: true },
    { range: '0-1', valid: false },
    { range: 'b=0-1', valid: false },
    { range: 'byte=0-1', valid: false },
    { range: 'bytes=-1', valid: false },
    { range: 'bytes=0--1', valid: false },
    { range: 'bytes=-1-0', valid: false },
    { range: 'bytes=a-9', valid: false },
    { range: 'bytes=10-9', valid: false },
    { range: 'bytes=a-a', valid: false },
];

function makeid(size) {
    let text = '';
    const possible =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < size; i += 1) {
        text += possible
            .charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}

function shuffle(array) {
    let randomIndex;
    let temporaryValue;
    const length = array.length;
    array.forEach((item, currentIndex, array) => {
        randomIndex = Math.floor(Math.random() * length);
        temporaryValue = array[currentIndex];
        // eslint-disable-next-line no-param-reassign
        array[currentIndex] = array[randomIndex];
        // eslint-disable-next-line no-param-reassign
        array[randomIndex] = temporaryValue;
    });
    return array;
}

function timeDiff(startTime) {
    const timeArray = process.hrtime(startTime);
    // timeArray[0] is whole seconds
    // timeArray[1] is remaining nanoseconds
    const milliseconds = (timeArray[0] * 1000) + (timeArray[1] / 1e6);
    return milliseconds;
}

function makeAuthInfo(accessKey) {
    const canIdMap = {
        accessKey1: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7'
            + 'cd47ef2be',
        accessKey2: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7'
            + 'cd47ef2bf',
        default: crypto.randomBytes(32).toString('hex'),
    };
    canIdMap[constants.publicId] = constants.publicId;

    return new AuthInfo({
        canonicalID: canIdMap[accessKey] || canIdMap.default,
        shortid: 'shortid',
        email: `${accessKey}@l.com`,
        accountDisplayName: `${accessKey}displayName`,
    });
}

class WebsiteConfig {
    constructor(indexDocument, errorDocument, redirectAllReqTo) {
        if (indexDocument) {
            this.IndexDocument = {};
            this.IndexDocument.Suffix = indexDocument;
        }
        if (errorDocument) {
            this.ErrorDocument = {};
            this.ErrorDocument.Key = errorDocument;
        }
        if (redirectAllReqTo) {
            this.RedirectAllRequestsTo = redirectAllReqTo;
        }
    }
    addRoutingRule(redirectParams, conditionParams) {
        const newRule = {};
        if (!this.RoutingRules) {
            this.RoutingRules = [];
        }
        if (redirectParams) {
            newRule.Redirect = {};
            Object.keys(redirectParams).forEach(key => {
                newRule.Redirect[key] = redirectParams[key];
            });
        }
        if (conditionParams) {
            newRule.Condition = {};
            Object.keys(conditionParams).forEach(key => {
                newRule.Condition[key] = conditionParams[key];
            });
        }
        this.RoutingRules.push(newRule);
    }
    getXml() {
        const xml = [];
        function _pushChildren(obj) {
            Object.keys(obj).forEach(element => {
                xml.push(`<${element}>${obj[element]}</${element}>`);
            });
        }

        xml.push('<WebsiteConfiguration xmlns=' +
            '"http://s3.amazonaws.com/doc/2006-03-01/">');

        if (this.IndexDocument) {
            xml.push('<IndexDocument>',
            `<Suffix>${this.IndexDocument.Suffix}</Suffix>`,
            '</IndexDocument>');
        }

        if (this.ErrorDocument) {
            xml.push('<ErrorDocument>',
            `<Key>${this.ErrorDocument.Key}</Key>`,
            '</ErrorDocument>');
        }

        if (this.RedirectAllRequestsTo) {
            xml.push('<RedirectAllRequestsTo>');
            if (this.RedirectAllRequestsTo.HostName) {
                xml.push('<HostName>',
                `${this.RedirectAllRequestsTo.HostName})`,
                '</HostName>');
            }
            if (this.RedirectAllRequestsTo.Protocol) {
                xml.push('<Protocol>',
                `${this.RedirectAllRequestsTo.Protocol})`,
                '</Protocol>');
            }
            xml.push('</RedirectAllRequestsTo>');
        }

        if (this.RoutingRules) {
            xml.push('<RoutingRules>');
            this.RoutingRules.forEach(rule => {
                xml.push('<RoutingRule>');
                if (rule.Condition) {
                    xml.push('<Condition>');
                    _pushChildren(rule.Condition);
                    xml.push('</Condition>');
                }
                if (rule.Redirect) {
                    xml.push('<Redirect>');
                    _pushChildren(rule.Redirect);
                    xml.push('</Redirect>');
                }
                xml.push('</RoutingRule>');
            });
            xml.push('</RoutingRules>');
        }

        xml.push('</WebsiteConfiguration>');
        return xml.join('');
    }
}

function createAlteredRequest(alteredItems, objToAlter,
    baseOuterObj, baseInnerObj) {
    const alteredRequest = Object.assign({}, baseOuterObj);
    const alteredNestedObj = Object.assign({}, baseInnerObj);
    Object.keys(alteredItems).forEach(key => {
        alteredNestedObj[key] = alteredItems[key];
    });
    alteredRequest[objToAlter] = alteredNestedObj;
    return alteredRequest;
}

function cleanup() {
    metadata.buckets = new Map;
    metadata.keyMaps = new Map;
    // Set data store array back to empty array
    ds.length = 0;
    // Set data store key count back to 1
    resetCount();
}

class DummyRequestLogger {

    constructor() {
        this.ops = [];
        this.counts = {
            trace: 0,
            debug: 0,
            info: 0,
            warn: 0,
            error: 0,
            fatal: 0,
        };
        this.defaultFields = {};
    }

    trace(msg) {
        this.ops.push(['trace', [msg]]);
        this.counts.trace += 1;
    }

    debug(msg) {
        this.ops.push(['debug', [msg]]);
        this.counts.debug += 1;
    }

    info(msg) {
        this.ops.push(['info', [msg]]);
        this.counts.info += 1;
    }

    warn(msg) {
        this.ops.push(['warn', [msg]]);
        this.counts.warn += 1;
    }

    error(msg) {
        this.ops.push(['error', [msg]]);
        this.counts.error += 1;
    }

    fatal(msg) {
        this.ops.push(['fatal', [msg]]);
        this.counts.fatal += 1;
    }

    getSerializedUids() {
        return 'dummy:Serialized:Uids';
    }

    addDefaultFields(fields) {
        Object.assign(this.defaultFields, fields);
    }

    end() {
        return this;
    }
}

class CorsConfigTester {
    constructor(params) {
        this._cors = [
          { allowedMethods: ['PUT', 'POST', 'DELETE'],
            allowedOrigins: ['http://www.example.com'],
            allowedHeaders: ['*'],
            maxAgeSeconds: 3000,
            exposeHeaders: ['x-amz-server-side-encryption'] },
          { id: 'testid',
            allowedMethods: ['GET'],
            allowedOrigins: ['*'],
            allowedHeaders: ['*'],
            maxAgeSeconds: 3000 },
        ];

        if (params) {
            Object.keys(params).forEach(key => {
                this._cors[0][key] = params[key];
            });
        }
    }

    getCors() {
        return this._cors;
    }

    constructXml() {
        const xml = [];
        xml.push('<CORSConfiguration>');
        this._cors.forEach(rule => {
            xml.push('<CORSRule>');
            ['allowedMethods', 'allowedOrigins', 'allowedHeaders',
            'exposeHeaders', 'maxAgeSeconds']
            .forEach(key => {
                if (rule[key] && Array.isArray(rule[key])) {
                    const element = key === 'maxAgeSeconds' ?
                    key.charAt(0).toUpperCase() + key.slice(1) :
                    key.charAt(0).toUpperCase() +
                    key.slice(1, -1);
                    rule[key].forEach(value => {
                        xml.push(`<${element}>${value}</${element}>`);
                    });
                }
            });
            if (rule.id) {
                xml.push(`<ID>${rule.id}</ID>`);
            }
            if (rule.maxAgeSeconds && !Array.isArray(rule.maxAgeSeconds)) {
                xml.push(`<MaxAgeSeconds>${rule.maxAgeSeconds}` +
                    '</MaxAgeSeconds>');
            }
            xml.push('</CORSRule>');
        });
        xml.push('</CORSConfiguration>');
        return xml.join('');
    }

    createBucketCorsRequest(method, bucketName, body) {
        const request = {
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?cors',
            query: { cors: '' },
        };
        if (method === 'PUT') {
            request.post = body || this.constructXml();
            request.headers['content-md5'] = crypto.createHash('md5')
                .update(request.post, 'utf8').digest('base64');
        }
        return request;
    }
}

const versioningTestUtils = {
    createPutObjectRequest: (bucketName, keyName, body) => {
        const params = {
            bucketName,
            namespace: 'default',
            objectKey: keyName,
            headers: {},
            url: `/${bucketName}/${keyName}`,
        };
        return new DummyRequest(params, body);
    },
    createBucketPutVersioningReq: (bucketName, status) => {
        const request = {
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?versioning',
            query: { versioning: '' },
        };
        const xml = '<VersioningConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<Status>${status}</Status>` +
        '</VersioningConfiguration>';
        request.post = xml;
        return request;
    },
    assertDataStoreValues: (ds, expectedValues) => {
        assert.strictEqual(ds.length, expectedValues.length + 1);
        for (let i = 0, j = 1; i < expectedValues.length; i++, j++) {
            if (expectedValues[i] === undefined) {
                assert.strictEqual(ds[j], expectedValues[i]);
            } else {
                assert.deepStrictEqual(ds[j].value, expectedValues[i]);
            }
        }
    },
};

class TaggingConfigTester {
    constructor() {
        this._tags = { k1: 'v1', k2: 'v2' };
    }

    getTags() {
        return this._tags;
    }

    constructXml() {
        const xml = [];
        xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
        '<Tagging> <TagSet>');
        Object.keys(this._tags).forEach(key => {
            const value = this._tags[key];
            xml.push(`<Tag><Key>${key}</Key><Value>${value}</Value></Tag>`);
        });
        xml.push('</TagSet> </Tagging>');
        return xml.join('');
    }

    createObjectTaggingRequest(method, bucketName, objectName, body) {
        const request = {
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            objectKey: objectName,
            url: '/?tagging',
            query: { tagging: '' },
        };
        if (method === 'PUT') {
            request.post = body || this.constructXml();
            request.headers['content-md5'] = crypto.createHash('md5')
                .update(request.post, 'utf8').digest('base64');
        }
        return request;
    }
}

class AccessControlPolicy {
    constructor(params) {
        this.Owner = {};
        this.Owner.ID = params.ownerID;
        this.Owner.DisplayName = params.ownerDisplayName;
        this.AccessControlList = [];
    }
    setOwnerID(ownerID) {
        this.Owner.ID = ownerID;
    }
    addGrantee(type, value, permission, displayName) {
        const grant = {
            Grantee: {
                Type: type,
                DisplayName: displayName,
            },
            Permission: permission,
        };
        if (type === 'AmazonCustomerByEmail') {
            grant.Grantee.EmailAddress = value;
        } else if (type === 'CanonicalUser') {
            grant.Grantee.ID = value;
        } else if (type === 'Group') {
            grant.Grantee.URI = value;
        }
        this.AccessControlList.push(grant);
    }
    getXml() {
        const xml = [];

        function _pushChildren(obj) {
            Object.keys(obj).forEach(element => {
                if (obj[element] !== undefined && element !== 'Type') {
                    xml.push(`<${element}>${obj[element]}</${element}>`);
                }
            });
        }
        xml.push('<AccessControlPolicy xmlns=' +
            '"http://s3.amazonaws.com/doc/2006-03-01/">', '<Owner>');
        _pushChildren(this.Owner);
        xml.push('</Owner>', '<AccessControlList>');
        this.AccessControlList.forEach(grant => {
            xml.push('<Grant>', `<Grantee xsi:type="${grant.Grantee.Type}">`);
            _pushChildren(grant.Grantee);
            xml.push('</Grantee>',
                `<Permission>${grant.Permission}</Permission>`,
                '</Grant>');
        });
        xml.push('</AccessControlList>', '</AccessControlPolicy>');
        return xml.join('');
    }
}

module.exports = {
    testsRangeOnEmptyFile,
    makeid,
    shuffle,
    timeDiff,
    createAlteredRequest,
    cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    WebsiteConfig,
    CorsConfigTester,
    versioningTestUtils,
    TaggingConfigTester,
    AccessControlPolicy,
};
