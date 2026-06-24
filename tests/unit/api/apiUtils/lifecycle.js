const assert = require('assert');
const { versioning } = require('arsenal');
const { validateMaxScannedEntries, decodeVersionIdMarker } =
      require('../../../../lib/api/apiUtils/object/lifecycle');

const versionIdUtils = versioning.VersionID;
// A valid encoded marker that round-trips to this internal version id.
const validInternalVid = '98765432109876999999PARIS00';
const validEncodedMarker = versionIdUtils.encode(validInternalVid);

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

describe('validateMaxScannedEntries helper', () => {
    tests.forEach(t => {
        it(t.it, () => {
            const result = validateMaxScannedEntries(t.params, t.config, t.minEntriesToBeScanned);
            assert.deepStrictEqual(result, t.expected);
        });
    });
});

describe('decodeVersionIdMarker helper', () => {
    ['null', '', undefined].forEach(vid => {
        it(`should treat ${JSON.stringify(vid)} as no marker`, () => {
            assert.strictEqual(decodeVersionIdMarker(vid), undefined);
        });
    });

    it('should decode a valid encoded marker', () => {
        assert.strictEqual(decodeVersionIdMarker(validEncodedMarker), validInternalVid);
    });

    it('should return InvalidArgument when decode returns an Error value', () => {
        // a malformed (non-null) marker that decode rejects by returning an Error
        const result = decodeVersionIdMarker('@@@bad@@@');
        assert(result instanceof Error);
        assert.strictEqual(result.message, 'InvalidArgument');
        assert.strictEqual(result.description, 'Invalid version id marker specified');
    });
});
