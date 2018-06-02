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
    return { $regex: new RegExp(pattern), $options: regexOpt };
}

module.exports = parseLikeExpression;
