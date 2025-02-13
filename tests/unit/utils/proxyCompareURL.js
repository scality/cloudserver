const assert = require('assert');
const arsenal = require('arsenal');
const { proxyCompareUrl } = arsenal.storage.data.external.backendUtils;

// TODO ARSN-464 remove the file: this function should be tested in Arsenal
const testCases = [
    {
        endpoint: 'test.scality.com',
        expRes: true,
        desc: 'NO_PROXY matches with middle wildcard',
    },
    {
        endpoint: 'test.scality.com',
        expRes: true,
        desc: 'NO_PROXY matches with beginning wildcard',
    },
    {
        endpoint: 'test.scality.com',
        expRes: true,
        desc: 'NO_PROXY matches with beginning period',
    },
    {
        endpoint: 'test.scality.com',
        expRes: true,
        desc: 'match with wildcard',
    },
    {
        endpoint: 'test.wrong.fr',
        expRes: false,
        desc: 'no match',
    },
];

describe('proxyCompareURL util function', () => {
    testCases.forEach(test => {
        it(`should return ${test.expRes} if ${test.desc}`, () => {
            const proxyMatch = proxyCompareUrl(test.endpoint);
            assert.strictEqual(test.expRes, proxyMatch);
        });
    });
});
