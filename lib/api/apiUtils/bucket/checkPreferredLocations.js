const { errors } = require('arsenal');

function checkPreferredLocations(location, locationConstraints, log) {
    const retError = loc => {
        const errMsg = 'value of the location you are attempting to set - ' +
        `${loc} - is not listed in the locationConstraint config`;
        log.trace(`locationConstraint is invalid - ${errMsg}`,
          { locationConstraint: loc });
        return errors.InvalidLocationConstraint.customizeDescription(errMsg);
    };
    if (typeof location === 'string' && !locationConstraints[location]) {
        return retError(location);
    }
    if (typeof location === 'object') {
        const { read, write } = location;
        if (!locationConstraints[read]) {
            return retError(read);
        }
        if (!locationConstraints[write]) {
            return retError(write);
        }
    }
    return null;
}

module.exports = checkPreferredLocations;
