const { config } = require('../../../../../lib/Config');

const isCEPH = process.env.CI_CEPH !== undefined;
const itSkipCeph = isCEPH ? it.skip : it;
const describeSkipIfCeph = isCEPH ? describe.skip : describe.skip; // always skip
let describeSkipIfNotMultiple = describe.skip;
let describeSkipIfNotMultipleOrCeph = describe.skip;

if (config.backends.data === 'multiple') {
    describeSkipIfNotMultiple = describe;
    describeSkipIfNotMultipleOrCeph = isCEPH ? describe.skip : describe.skip; // always skip
}

function hasLocation(lc) {
    return config.locationConstraints[lc] !== undefined;
}

module.exports = {
    isCEPH,
    itSkipCeph,
    describeSkipIfCeph,
    describeSkipIfNotMultiple,
    describeSkipIfNotMultipleOrCeph,
    hasLocation,
};
