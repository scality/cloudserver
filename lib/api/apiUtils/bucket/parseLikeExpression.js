/**
 * parse LIKE expressions
 * @param {string} regex - regex pattern
 * @return {object} MongoDB search object
 */
function parseLikeExpression(regex) {
    const split = regex.split('/');
    if (split.length < 3) {
        return { $regex: regex };
    }
    const pattern = split.slice(1, split.length - 1).join('/');
    const regexOpt = split[split.length - 1];
    return { $regex: pattern, $options: regexOpt };
}

module.exports = parseLikeExpression;
