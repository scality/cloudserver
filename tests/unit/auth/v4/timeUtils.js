import assert from 'assert';

import { convertAmzTimeToMs, convertUTCtoISO8601, }
    from '../../../../lib/auth/v4/timeUtils';

describe('convertAmzTimeToMs function', () => {
    it('should convert ISO8601Timestamp format without ' +
    'dashes or colons, e.g. 20160202T220410Z to milliseconds since ' +
    'Unix epoch', () => {
        const input = '20160202T220410Z';
        const expectedOutput = 1454450650000;
        const actualOutput = convertAmzTimeToMs(input);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});

describe('convertUTCtoISO8601 function', () => {
    it('should UTC timestamp to ISO8601 timestamp', () => {
        const input = 'Sun, 08 Feb 2015 20:14:05 GMT';
        const expectedOutput = '20150208T201405Z';
        const actualOutput = convertUTCtoISO8601(input);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});
