const assert = require('assert');
const aclUtils = require('../../../lib/utilities/aclUtils');


describe('checkGrantHeaderValidity for acls', () => {
    const tests = [
        {
            it: 'should allow valid x-amz-grant-read grant',
            headers: {
                'x-amz-grant-read':
                'uri=http://acs.amazonaws.com/groups/global/AllUsers',
            },
            result: true,
        },
        {
            it: 'should allow valid x-amz-grant-write grant',
            headers: {
                'x-amz-grant-write':
                'emailaddress=user2@example.com',
            },
            result: true,
        },
        {
            it: 'should allow valid x-amz-grant-read-acp grant',
            headers: {
                'x-amz-grant-read-acp':
                'emailaddress=superuser@example.com',
            },
            result: true,
        },
        {
            it: 'should allow valid x-amz-grant-write-acp grant',
            headers: {
                'x-amz-grant-write-acp':
                'id=79a59df900b949e55d96a1e6' +
                '98fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            },
            result: true,
        },
        {
            it: 'should allow valid x-amz-grant-full-control grant',
            headers: {
                'x-amz-grant-full-control':
                'id=79a59df900b949e55d96a1e6' +
                '98fbacedfd6e09d98eacf8f8d5218e7cd47ef2be,' +
                'emailaddress=foo@bar.com',
            },
            result: true,
        },
        {
            it: 'should deny grant without equal sign',
            headers: {
                'x-amz-grant-full-control':
                'id79a59df900b949e55d96a1e6' +
                '98fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            },
            result: false,
        },
        {
            it: 'should deny grant with bad uri',
            headers: {
                'x-amz-grant-full-control':
                'uri=http://totallymadeup',
            },
            result: false,
        },
        {
            it: 'should deny grant with bad emailaddress',
            headers: {
                'x-amz-grant-read':
                'emailaddress=invalidemail.com',
            },
            result: false,
        },
        {
            it: 'should deny grant with bad canonicalID',
            headers: {
                'x-amz-grant-write':
                'id=123',
            },
            result: false,
        },
        {
            it: 'should deny grant with bad type of identifier',
            headers: {
                'x-amz-grant-write':
                'madeupidentifier=123',
            },
            result: false,
        },
    ];

    tests.forEach(test => {
        it(test.it, () => {
            const actualResult =
                aclUtils.checkGrantHeaderValidity(test.headers);
            assert.strictEqual(actualResult, test.result);
        });
    });
});
