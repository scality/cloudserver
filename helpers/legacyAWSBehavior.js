const { config } = require('../Config');

/**
 * check the legacy AWS behaviour
 * For new configuration: check if locationConstraint has its
 * legacyAwsBehavior put to true
 * @param {string | undefined} locationConstraint - value of user
 * metadata location constraint header or bucket location constraint
 * @return {boolean} - true if valid, false if not
 */

function isLegacyAwsBehavior(locationConstraint) {
    return (config.locationConstraints[locationConstraint] &&
      config.locationConstraints[locationConstraint].legacyAwsBehavior);
}

module.exports = isLegacyAwsBehavior;
