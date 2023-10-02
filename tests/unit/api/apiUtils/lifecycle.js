const assert = require('assert');
const { validateMaxScannedEntries } =
      require('../../../../lib/api/apiUtils/object/lifecycle');

const tests = [
    {
        it: 'should return config value if no query params set',
        config: { maxScannedLifecycleListingEntries: 10000 },
        params: {},
        minEntriesToBeScanned: 3,
        expected: { isValid: true, maxScannedLifecycleListingEntries: 10000 },
    },
    {
        it: 'should validate when query param is within the allowed range',
        config: { maxScannedLifecycleListingEntries: 10000 },
        params: { 'max-scanned-lifecycle-listing-entries': '5000' },
        minEntriesToBeScanned: 3,
        expected: { isValid: true, maxScannedLifecycleListingEntries: 5000 },
    },
    {
        it: 'should return invalid when query param is not a number',
        config: { maxScannedLifecycleListingEntries: 10000 },
        params: { 'max-scanned-lifecycle-listing-entries': 'invalid' },
        minEntriesToBeScanned: 3,
        expected: { isValid: false },
    },
    {
        it: 'should return invalid when query param is less than min',
        config: { maxScannedLifecycleListingEntries: 10000 },
        params: { 'max-scanned-lifecycle-listing-entries': '1' },
        minEntriesToBeScanned: 3,
        expected: { isValid: false },
    },
    {
        it: 'should return invalid when query param exceeds config value',
        config: { maxScannedLifecycleListingEntries: 10000 },
        params: { 'max-scanned-lifecycle-listing-entries': '15000' },
        minEntriesToBeScanned: 3,
        expected: { isValid: false },
    },
];

describe('getReplicationInfo helper', () => {
    tests.forEach(t => {
        it(t.it, () => {
            const result = validateMaxScannedEntries(t.params, t.config, t.minEntriesToBeScanned);
            assert.deepStrictEqual(result, t.expected);
        });
    });
});
