import assert from 'assert';

import awsURIencode from '../../../../lib/auth/v4/awsURIencode';

// Note that expected outputs came from running node aws-sdk's
// AWS.util.uriEscapePath and AWS.util.uriEscape functions
// (see aws-sdk lib/signers/v4.js)
describe('should URIencode in accordance with AWS rules', () => {
    it('should not encode / if give false argument', () => {
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

    it('should encode / if no second argument given', () => {
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

    it('should encode native language characters', () => {
        const input = '/s3amazonaws.com/Pâtisserie=中文-español-English' +
        '-हिन्दी-العربية-português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const expectedOutput = '%2Fs3amazonaws.com%2FP%C3%A2tisserie%3D%E4' +
        '%B8%AD%E6%96%87-espa%C3%B1ol-English-%E0%A4%B9%E0%A4%BF%E0%A4%A8%E0' +
        '%A5%8D%E0%A4%A6%E0%A5%80-%D8%A7%D9%84%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9' +
        '-portugu%C3%AAs-%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE-%D1%80' +
        '%D1%83%D1%81%D1%81%D0%BA%D0%B8%D0%B9-%E6%97%A5%E6%9C%AC%E8%AA%9E-%E0' +
        '%A8%AA%E0%A9%B0%E0%A8%9C%E0%A8%BE%E0%A8%AC%E0%A9%80-%ED%95%9C%EA%B5' +
        '%AD%EC%96%B4-%E0%AE%A4%E0%AE%AE%E0%AE%BF%E0%AE%B4%E0%AF%8D';
        const actualOutput = awsURIencode(input);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});
