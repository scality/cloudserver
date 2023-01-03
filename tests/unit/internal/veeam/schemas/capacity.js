const assert = require('assert');
const validateCapacitySchema = require('../../../../../lib/routes/veeam/schemas/capacity');
const { errors } = require('arsenal');

describe('RouteVeeam: validateCapacitySchema', () => {
    [
        {},
        null,
        undefined,
        '',
        {
            Capacity: -2,
        },
        {
            Capacity: -2,
            Available: 0,
            Used: 0,
        },
        {
            CapacityInfo: {
                Capacity: -2,
                Available: 0,
                Used: 0,
            },
        },
    ].forEach(test => {
        it(`should return MalformedXML for ${JSON.stringify(test)}`, () => {
            assert.throws(() => validateCapacitySchema(test).message, errors.MalformedXML.message);
        });
    });

    [
        {
            CapacityInfo: {
                Capacity: '65465',
                Available: '6541650',
                Used: '10156',
            },
        },
        {
            CapacityInfo: {
                Capacity: 1,
                Available: 0,
                Used: 0,
            },
        },
        {
            CapacityInfo: {
                Capacity: -1,
                Available: 0,
                Used: 0,
            },
        },
    ].forEach(test => {
        it(`should validate scheme for ${JSON.stringify(test)}`, () => {
            assert.doesNotThrow(() => validateCapacitySchema(test));
        });
    });
});
