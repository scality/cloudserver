const assert = require('assert');
const proxyCompareUrl = require('../../../lib/data/proxyCompareUrl');

const testCases = [
    {
        endpoint: 'test.scality.com',
        noProxy: '',
        expRes: false,
        desc: 'no NO_PROXY env var set',
    },
    {
        endpoint: 'test.scality.com',
        noProxy: 'test.*.com',
        expRes: true,
        desc: 'NO_PROXY matches with middle wildcard',
    },
    {
        endpoint: 'test.scality.com',
        noProxy: '*.com',
        expRes: true,
        desc: 'NO_PROXY matches with beginning wildcard',
    },
    {
        endpoint: 'test.scality.com',
        noProxy: '.scality.com',
        expRes: true,
        desc: 'NO_PROXY matches with beginning period',
    },
    {
        endpoint: 'test.scality.com',
        noProxy: 'test.nomatch,test.scality.*',
        expRes: true,
        desc: 'match with wildcard',
    },
    {
        endpoint: 'test.scality.com',
        noProxy: 'test.nomatch,no.scality.no,no.*.com,scality.com',
        expRes: false,
        desc: 'no match',
    },
];

describe('proxyCompareURL util function', () => {
    testCases.forEach(test => {
        it(`should return ${test.expRes} if ${test.desc}`, done => {
            process.env.NO_PROXY = test.noProxy;
            const proxyMatch = proxyCompareUrl(test.endpoint);
            assert.strictEqual(test.expRes, proxyMatch);
            done();
        });
    });

    after(() => {
        process.env.NO_PROXY = '';
    });
});
