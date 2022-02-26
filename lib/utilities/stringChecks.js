/**
 * hasNonPrintables returns whether or not the given value
 * includes characters that are non-printable.
 *
 * @param {string} value - value to check for non-printables
 * @returns {Boolean} whether or not the value has non-printables
 */
function hasNonPrintables(value) {
    for (const char of value) {
        const codePoint = char.codePointAt(0);
        if (codePoint < 32 || codePoint === 127) {
            return true;
        }
    }
    return false;
}

module.exports = {
    hasNonPrintables,
};
