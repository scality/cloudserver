/**
 * @param {string or object} toCheck - the item to be JSON parsed or returned
 * @return {object} error object if JSON.parse fails, JSON.parsed string if
 * toCheck was a valid JSON string or toCheck if toCheck was not a string
 */

function checkStringParse(toCheck) {
    if (typeof toCheck === 'string') {
        try {
            return JSON.parse(toCheck);
        } catch (e) {
            return e;
        }
    }
    return toCheck;
}

export default checkStringParse;
