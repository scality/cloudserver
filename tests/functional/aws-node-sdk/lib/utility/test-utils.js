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

/**
 * We consider cold storage & restore supported if Transition lifecycle rule is supported.
 * In our implementation, it's the only way to move objects to cold storage (can't write directly to cold)
 * Even if Transition could be used without cold storage location (hot transition), but both features are
 * enabled together at the moment.
*/
const hasColdStorage = config.supportedLifecycleRules.some(rule => rule.endsWith('Transition'));

module.exports = {
    isCEPH,
    itSkipCeph,
    describeSkipIfCeph,
    describeSkipIfNotMultiple,
    describeSkipIfNotMultipleOrCeph,
    hasLocation,
    hasColdStorage,
};
