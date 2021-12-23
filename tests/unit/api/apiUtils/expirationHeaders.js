const assert = require('assert');
const { LifecycleDateTime } = require('arsenal').s3middleware.lifecycleHelpers;


const {
    generateExpirationHeaders,
} = require('../../../../lib/api/apiUtils/object/expirationHeaders');

const datetime = new LifecycleDateTime();
const objectDate = 'Fri, 21 Dec 2012 00:00:00 GMT';
const expectedDaysExpiryDate = 'Sat, 22 Dec 2012 00:00:00 GMT';
const expectedDateExpiryDate = 'Mon, 24 Dec 2012 00:00:00 GMT';


const lifecycleExpirationDays = {
    rules: [
        {
            ruleID: 'test-days',
            ruleStatus: 'Enabled',
            actions: [
                { actionName: 'Expiration', days: 1 },
            ],
            prefix: '',
        },
    ],
};

const lifecycleExpirationTags = {
    rules: [
        {
            ruleID: 'test-tags',
            ruleStatus: 'Enabled',
            filters: {
                tags: [
                    { key: 'key1', val: 'val1' },
                ],
            },
            actions: [
                { actionName: 'Expiration', days: 1 },
            ],
        },
    ],
};

const lifecycleExpirationDate = {
    rules: [
        {
            ruleID: 'test-date',
            ruleStatus: 'Enabled',
            actions: [
                { actionName: 'Expiration', date: 'Mon, 24 Dec 2012 00:00:00 GMT' },
            ],
            prefix: '',
        },
    ],
};

const lifecycleExpirationMPU = {
    rules: [
        {
            ruleID: 'test-mpu',
            ruleStatus: 'Enabled',
            actions: [
                { actionName: 'AbortIncompleteMultipartUpload', days: 1 },
            ],
            prefix: '',
        },
    ],
};

const lifecycleExpirationNotApplicable = {
    rules: [
        {
            ruleID: 'test-mpu',
            ruleStatus: 'Enabled',
            actions: [
                { actionName: 'AbortIncompleteMultipartUpload', days: 1 },
                { actionName: 'Expiration', date: 'Mon, 24 Dec 2012 00:00:00 GMT' },
            ],
            prefix: 'noapplyprefix/',
        },
    ],
};

describe('generateExpirationHeaders', () => {
    const tests = [
        [
            'should return correct headers when object/mpu params are missing',
            { lifecycleConfig: lifecycleExpirationDays },
            {},
        ],
        [
            'should return correct headers when lifecycle config is missing',
            { objectParams: { key: 'object', date: objectDate, tags: {} } },
            {},
        ],
        [
            'should return correct headers when request is for a versioned objects',
            {
                lifecycleConfig: lifecycleExpirationDays,
                objectParams: { key: 'object', date: objectDate, tags: {} },
                isVersionedReq: true,
            },
            {},
        ],
        [
            'should return correct headers for object (days)',
            {
                lifecycleConfig: lifecycleExpirationDays,
                objectParams: { key: 'object', date: objectDate, tags: {} },
            },
            {
                'x-amz-expiration': `expiry-date="${expectedDaysExpiryDate}", rule-id="test-days"`,
            },
        ],
        [
            'should return correct headers for object (date)',
            {
                lifecycleConfig: lifecycleExpirationDate,
                objectParams: { key: 'object', date: objectDate, tags: {} },
            },
            {
                'x-amz-expiration': `expiry-date="${expectedDateExpiryDate}", rule-id="test-date"`,
            },
        ],
        [
            'should return correct headers for object (days with tags)',
            {
                lifecycleConfig: lifecycleExpirationTags,
                objectParams: { key: 'object', date: objectDate, tags: { key1: 'val1' } },
            },
            {
                'x-amz-expiration': `expiry-date="${expectedDaysExpiryDate}", rule-id="test-tags"`,
            },
        ],
        [
            'should return correct headers when zero expiration rules apply',
            {
                lifecycleConfig: lifecycleExpirationNotApplicable,
                objectParams: { key: 'object', date: objectDate, tags: {} },
            },
            {},
        ],
        [
            'should return correct headers for mpu',
            {
                lifecycleConfig: lifecycleExpirationMPU,
                mpuParams: { key: 'object', date: objectDate },
            },
            {
                'x-amz-abort-date': expectedDaysExpiryDate,
                'x-amz-abort-rule-id': 'test-mpu',
            },
        ],
        [
            'should return correct headers when zero mpu expiration rules apply',
            {
                lifecycleConfig: lifecycleExpirationNotApplicable,
                mpuParams: { key: 'object', date: objectDate },
            },
            {},
        ],
    ];

    tests.forEach(([msg, params, expected]) => it(msg, () => {
        assert.deepStrictEqual(generateExpirationHeaders(params, datetime), expected);
    }));
});
