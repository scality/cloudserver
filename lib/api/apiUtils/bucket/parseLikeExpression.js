/**
 * parse LIKE expressions
 * @param {string} regex - regex pattern
 * @return {object} MongoDB search object
 */
function parseLikeExpression(regex) {
    if (typeof regex !== 'string') {
        return null;
    }
    const split = regex.split('/');
    if (split.length < 3 || split[0] !== '') {
        return { $regex: regex };
    }
    const pattern = split.slice(1, split.length - 1).join('/');
    const regexOpt = split[split.length - 1];
    // Escape regex special characters to prevent ReDoS
    const escapedPattern = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return { $regex: new RegExp(escapedPattern), $options: regexOpt };
}

module.exports = parseLikeExpression;
