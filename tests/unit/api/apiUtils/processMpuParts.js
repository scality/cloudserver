const assert = require('assert');
const { Logger } = require('werelogs');
const log = new Logger('S3').newRequestLogger();
const { validateAndFilterMpuParts } =
    require('../../../../lib/api/apiUtils/object/processMpuParts');

let storedParts;
let jsonList;

// r - number of parts to remove
function _buildExpectedResult(r) {
    const result = [];
    for (let i = 0; i < r; i++) {
        jsonList.Part.shift();
        result.push(...storedParts[i].value.partLocations);
    }
    return result;
}

describe('processMpuParts::validateAndFilterMpuParts', () => {
    let mpuOverviewKey;
    let splitter;
    beforeEach(() => {
        mpuOverviewKey =
            '"overview..|..fred..|..8e51eecb51ca4caa96dc4ebd51514f2a"';
        splitter = '..|..';
        storedParts = require('./storedParts');
        jsonList = require('./jsonList');
    });
    afterEach(() => {
        mpuOverviewKey = null;
        splitter = null;
    });

    [0, 2, 4].forEach(n => {
        it(`should filter ${n} parts that are not used in complete mpu`,
            () => {
                const expected = _buildExpectedResult(n);

                const result = validateAndFilterMpuParts(storedParts, jsonList,
                    mpuOverviewKey, splitter, log);
                assert.deepStrictEqual(expected, result.extraPartLocations);
            });
    });
});
