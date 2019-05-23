const { errors } = require('arsenal');
const xml2js = require('xml2js');

/**
 * Handle initial parsing of XML using the `xml2js.parseString` method
 * @param {string} xml - The XML to be parsed
 * @param {object} log - Werelogs logger
 * @param {function} cb - Callback to call
 * @return {undefined}
 */
function parseXML(xml, log, cb) {
    if (!xml) {
        log.debug('request xml is missing');
        return cb(errors.MalformedXML);
    }
    return xml2js.parseString(xml, (err, result) => {
        if (err) {
            log.debug('request xml is malformed');
            return cb(errors.MalformedXML);
        }
        return cb(null, result);
    });
}

module.exports = parseXML;
