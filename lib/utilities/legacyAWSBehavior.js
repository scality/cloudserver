import config from '../Config';

/**
 * check the legacy AWS behaviour
 * For new configuration: check if locationConstraint has its
 * legacyAwsBehavior put to true
 * For old configuration: check if locationConstraint is 'us-east-1' and
 * usEastBehavior is true
 * @param {string | undefined} locationConstraint - value of user
 * metadata location constraint header or bucket location constraint
 * @param {boolean} usEastBehavior - only used for old configuration
 * @return {boolean} - true if valid, false if not
 */

export default function isLegacyAwsBehavior(locationConstraint,
  usEastBehavior) {
    return (config.locationConstraints &&
      config.locationConstraints[locationConstraint] &&
      config.locationConstraints[locationConstraint].legacyAwsBehavior)
      || (locationConstraint === 'us-east-1' && usEastBehavior);
}
