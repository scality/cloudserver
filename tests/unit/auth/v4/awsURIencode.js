import assert from 'assert';

import awsURIencode from '../../../../lib/auth/v4/awsURIencode';

// Note that expected outputs came from running node aws-sdk's
// AWS.util.uriEscapePath and AWS.util.uriEscape functions
// (see aws-sdk lib/signers/v4.js)
describe('awsURIencode function', () => {
    it('should URIencode in accordance with AWS rules ' +
        'and should not encode / if give false argument', () => {
        const input1 = '/s3amazonaws.com/?$*@whateverASFEFWE()@)(*#@+  )';
        const expectedOutput1 = '/s3amazonaws.com/%3F%24%2A%40whatever' +
            'ASFEFWE%28%29%40%29%28%2A%23%40%2B%20%20%29';
        const actualOutput1 = awsURIencode(input1, false);
        assert.strictEqual(actualOutput1, expectedOutput1);
        const input2 = 'jfwwe w55%% ljwelj SE#3ifo;/sihr3;f399evij' +
            'cwn  k#@@#/ R#@) seifjF$E&&+@+**!^#*KL#Jsjcoi3(!)(#89u)';
        const expectedOutput2 = 'jfwwe%20w55%25%25%20ljwelj%20SE%233ifo%3B' +
            '/sihr3%3Bf399evijcwn%20%20k%23%40%40%23/%20R%23%40%29%20seifjF%' +
            '24E%26%26%2B%40%2B%2A%2A%21%5E%23%2AKL%23Jsjcoi3%28%' +
            '21%29%28%2389u%29';
        const actualOutput2 = awsURIencode(input2, false);
        assert.strictEqual(actualOutput2, expectedOutput2);
    });

    it('should URIencode in accordance with AWS rules ' +
        'and should encode / if no second argument given', () => {
        const input1 = '/s3amazonaws.com/?$*@whateverASFEFWE()@)(*#@+  )';
        const expectedOutput1 = '%2Fs3amazonaws.com%2F%3F%24%2A%40whatever' +
            'ASFEFWE%28%29%40%29%28%2A%23%40%2B%20%20%29';
        const actualOutput1 = awsURIencode(input1);
        assert.strictEqual(actualOutput1, expectedOutput1);
        const input2 = 'jfwwe w55%% ljwelj SE#3ifo;/sihr3;f399evij' +
            'cwn  k#@@#/ R#@) seifjF$E&&+@+**!^#*KL#Jsjcoi3(!)(#89u)';
        const expectedOutput2 = 'jfwwe%20w55%25%25%20ljwelj%20SE%233if' +
            'o%3B%2Fsihr3%3Bf399evijcwn%20%20k%23%40%40%23%2F%20R%23%40%' +
            '29%20seifjF%24E%26%26%2B%40%2B%2A%2A%21%5E%23%2AKL%23Jsjcoi3' +
            '%28%21%29%28%2389u%29';
        const actualOutput2 = awsURIencode(input2);
        assert.strictEqual(actualOutput2, expectedOutput2);
    });
});
