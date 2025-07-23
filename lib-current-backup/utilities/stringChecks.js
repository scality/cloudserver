/**
 * hasNonPrintables returns whether or not the given value
 * includes characters that are non-printable.
 *
 * @param {string} value - value to check for non-printables
 * @returns {Boolean} whether or not the value has non-printables
 */
function hasNonPrintables(value) {
    return Array.from(value).some(char => {
        const codePoint = char.codePointAt(0);
        return codePoint < 32 || codePoint === 127;
    });
}

module.exports = {
    hasNonPrintables,
};
