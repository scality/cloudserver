/**
 * Removes the given elements from a comma-separated header value, along with
 * their separator, wherever they appear. The other elements are kept as sent.
 * Pass '' to remove empty elements, which recipients must ignore
 * (RFC 9110, section 5.6.1).
 * @param {string} value - comma-separated header value
 * @param {...string} elements - elements to remove, compared after trimming
 * @return {string} the value without those elements, trimmed
 */
function removeFromCommaSeparatedList(value, ...elements) {
    if (typeof value !== 'string') {
        return value;
    }
    return value
        .split(',')
        .filter(item => !elements.includes(item.trim()))
        .join(',')
        .trim();
}

module.exports = removeFromCommaSeparatedList;
