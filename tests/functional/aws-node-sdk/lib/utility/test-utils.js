const { config } = require('../../../../../lib/Config');

let describeSkipIfNotMultiple = describe.skip;

if (config.backends.data === 'multiple') {
    describeSkipIfNotMultiple = describe;
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
    describeSkipIfNotMultiple,
    hasLocation,
    hasColdStorage,
};
