import assert from 'assert';
import lolex from 'lolex';

import { checkTimeSkew, convertAmzTimeToMs, convertUTCtoISO8601 }
    from '../../../../lib/auth/v4/timeUtils';
import { DummyRequestLogger } from '../../helpers';
const log = new DummyRequestLogger();

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

describe('checkTimeSkew function', () => {
    let clock;
    before(() => {
        // Time is 2016-03-17T18:22:01.033Z
        clock = lolex.install(1458238921033);
    });
    after(() => {
        clock.uninstall();
    });

    // Our default expiry for header auth check is 15 minutes (in secs)
    const expiry = (15 * 60);
    it('should allow requests with timestamps under 15 minutes ' +
        'in the future', () => {
        const timestamp14MinInFuture = '20160317T183601033Z';
        const expectedOutput = false;
        const actualOutput = checkTimeSkew(timestamp14MinInFuture,
            expiry, log);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    it('should not allow requests with timestamps more than 15 minutes ' +
        'in the future', () => {
        const timestamp16MinInFuture = '20160317T183801033Z';
        const expectedOutput = true;
        const actualOutput = checkTimeSkew(timestamp16MinInFuture,
            expiry, log);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    it('should allow requests with timestamps earlier than the ' +
        'the expiry', () => {
        const timestamp14MinInPast = '20160317T180801033Z';
        const expectedOutput = false;
        const actualOutput = checkTimeSkew(timestamp14MinInPast,
            expiry, log);
        assert.strictEqual(actualOutput, expectedOutput);
    });

    it('should not allow requests with timestamps later ' +
        'than the expiry', () => {
        const timestamp16MinInPast = '20160317T180601033Z';
        const expectedOutput = true;
        const actualOutput = checkTimeSkew(timestamp16MinInPast,
            expiry, log);
        assert.strictEqual(actualOutput, expectedOutput);
    });
});
