const assert = require('assert');
const { errors } = require('arsenal');

const locationHeaderCheck =
    require('../../../lib/api/apiUtils/object/locationHeaderCheck');

const objectKey = 'locationHeaderCheckObject';
const bucketName = 'locationHeaderCheckBucket';

const testCases = [
    {
        location: 'doesnotexist',
        expRes: errors.InvalidLocationConstraint.customizeDescription(
            'Invalid location constraint specified in header'),
    }, {
        location: '',
        expRes: undefined,
    }, {
        location: 'awsbackend',
        expRes: {
            location: 'awsbackend',
            key: objectKey,
            locationType: 'aws_s3',
        },
    }, {
        location: 'awsbackendmismatch',
        expRes: {
            location: 'awsbackendmismatch',
            key: `${bucketName}/${objectKey}`,
            locationType: 'aws_s3',
        },
    },
];

describe('Location Header Check', () => {
    testCases.forEach(test => {
        it('should return expected result with location constraint header ' +
        `set to ${test.location}`, () => {
            const headers = { 'x-amz-location-constraint': `${test.location}` };
            const checkRes =
                locationHeaderCheck(headers, objectKey, bucketName);
            assert.deepStrictEqual(checkRes, test.expRes);
        });
    });
});
