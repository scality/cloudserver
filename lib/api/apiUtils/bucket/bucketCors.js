import { parseString } from 'xml2js';

import { errors } from 'arsenal';

/*
    Format of xml request:

    <CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>http://www.example.com</AllowedOrigin>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedMethod>POST</AllowedMethod>
            <AllowedMethod>DELETE</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <MaxAgeSeconds>3000</MaxAgeSec>
            <ExposeHeader>x-amz-server-side-encryption</ExposeHeader>
        </CORSRule>
        <CORSRule>
            <AllowedOrigin>*</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <MaxAgeSeconds>3000</MaxAgeSeconds>
        </CORSRule>
    </CORSConfiguration>
*/

const customizedErrs = {
    numberRules: 'The number of CORS rules should not exceed allowed limit ' +
    'of 100 rules.',
    originAndMethodExist: 'Each CORSRule must identify at least one origin ' +
    'and one method.',
};

// Helper validation methods
export const _validator = {
    /** _validator.validateNumberWildcards - check if string has multiple
    *   wildcards
    @param {string} string - string to check for multiple wildcards
    @return {boolean} - whether more than one wildcard in string
    */
    validateNumberWildcards(string) {
        const firstIndex = string.indexOf('*');
        if (firstIndex !== -1) {
            return (string.indexOf('*', firstIndex + 1) === -1);
        }
        return true;
    },
    /** _validator.validateID - check value of optional ID
    * @param {string[]} id - array containing id string
    * @return {(Error|true|undefined)} - Arsenal error on failure, true on
    *   success, undefined if ID does not exist
    */
    validateID(id) {
        if (!id) {
            return undefined; // to indicate ID does not exist
        }
        if (!Array.isArray(id) || id.length !== 1
        || typeof id[0] !== 'string') {
            return errors.MalformedXML;
        }
        if (id[0] === '') {
            return undefined;
        }
        return true;
    },
    /** _validator.validateMaxAgeSeconds - check value of optional MaxAgeSeconds
    * @param {string[]} seconds - array containing number string
    * @return {(Error|parsedValue|undefined)} - Arsenal error on failure, parsed
    *   value if valid, undefined if MaxAgeSeconds does not exist
    */
    validateMaxAgeSeconds(seconds) {
        if (!seconds) {
            return undefined;
        }
        if (!Array.isArray(seconds) || seconds.length !== 1) {
            return errors.MalformedXML;
        }
        if (seconds[0] === '') {
            return undefined;
        }
        const parsedValue = parseInt(seconds[0], 10);
        const errMsg = `MaxAgeSeconds "${seconds[0]}" is not a valid value.`;
        if (isNaN(parsedValue) || parsedValue < 0) {
            return errors.MalformedXML.customizeDescription(errMsg);
        }
        return parsedValue;
    },
    /** _validator.validateNumberRules - return if number of rules exceeds 100
    * @param {number} length - array containing number string
    * @return {(Error|true)} - Arsenal error on failure, true on success
    */
    validateNumberRules(length) {
        if (length > 100) {
            return errors.InvalidRequest
            .customizeDescription(customizedErrs.numberRules);
        }
        return true;
    },
    /** _validator.validateOriginAndMethodExist
    * @param {string[]} allowedMethods - array of AllowedMethod's
    * @param {string[]} allowedOrigins - array of AllowedOrigin's
    * @return {(Error|true)} - Arsenal error on failure, true on success
    */
    validateOriginAndMethodExist(allowedMethods, allowedOrigins) {
        if (allowedOrigins && allowedMethods &&
        Array.isArray(allowedOrigins) &&
        Array.isArray(allowedMethods) &&
        allowedOrigins.length > 0 &&
        allowedMethods.length > 0) {
            return true;
        }
        return errors.MalformedXML
            .customizeDescription(customizedErrs.originAndMethodExist);
    },
    /** _validator.validateMethods - check values of AllowedMethod's
    * @param {string[]} methods - array of AllowedMethod's
    * @return {(Error|true)} - Arsenal error on failure, true on success
    */
    validateMethods(methods) {
        let invalidMethod;
        function isValidMethod(method) {
            const acceptedValues = ['GET', 'PUT', 'HEAD', 'POST', 'DELETE'];
            if (acceptedValues.indexOf(method) !== -1) {
                return true;
            }
            invalidMethod = method;
            return false;
        }
        if (!methods.every(isValidMethod)) {
            const errMsg = 'Found unsupported HTTP method in CORS config. ' +
            `Unsupported method is "${invalidMethod}"`;
            return errors.InvalidRequest.customizeDescription(errMsg);
        }
        return true;
    },
    /** _validator.validateAllowedOriginsOrHeaders - check values
    * @param {string[]} elementArr - array of elements to check
    * @param {string} typeElement - type of element being checked
    * @return {(Error|true)} - Arsenal error on failure, true on success
    */
    validateAllowedOriginsOrHeaders(elementArr, typeElement) {
        for (let i = 0; i < elementArr.length; i++) {
            const element = elementArr[i];
            if (typeof element !== 'string' || element === '') {
                return errors.MalformedXML;
            }
            if (!this.validateNumberWildcards(element)) {
                const errMsg = `${typeElement} "${element}" can not have ` +
                'more than one wildcard.';
                return errors.InvalidRequest.customizeDescription(errMsg);
            }
        }
        return true;
    },
    /** _validator.validateAllowedHeaders - check values of AllowedHeader's
    * @param {string[]} headers - array of AllowedHeader's
    * @return {(Error|true|undefined)} - Arsenal error on failure, true if
    *   valid, undefined if optional AllowedHeader's do not exist
    */
    validateAllowedHeaders(headers) {
        if (!headers) {
            return undefined; // to indicate AllowedHeaders do not exist
        }
        if (!Array.isArray(headers) || headers.length === 0) {
            return errors.MalformedXML;
        }
        const result =
            this.validateAllowedOriginsOrHeaders(headers, 'AllowedHeader');
        if (result instanceof Error) {
            return result;
        }
        return true;
    },
    /** _validator.validateExposeHeaders - check values of ExposeHeader's
    * @param {string[]} headers - array of ExposeHeader's
    * @return {(Error|true|undefined)} - Arsenal error on failure, true if
    *   valid, undefined if optional ExposeHeader's do not exist
    */
    validateExposeHeaders(headers) {
        if (!headers) {
            return undefined; // indicate ExposeHeaders do not exist
        }
        if (!Array.isArray(headers) || headers.length === 0) {
            return errors.MalformedXML;
        }
        for (let i = 0; i < headers.length; i++) {
            const header = headers[i];
            if (typeof header !== 'string') {
                return errors.MalformedXML;
            }
            if (header.indexOf('*') !== -1) {
                const errMsg = `ExposeHeader ${header} contains a wildcard. ` +
                'Wildcards are currently not supported for ExposeHeader.';
                return errors.InvalidRequest.customizeDescription(errMsg);
            }
            if (!/^[A-Za-z0-9-]*$/.test(header)) {
                const errMsg = `ExposeHeader ${header} contains invalid ` +
                'character.';
                return errors.InvalidRequest.customizeDescription(errMsg);
            }
        }
        return true;
    },
};

/** _validateCorsXml - Validate XML, returning an error if any part is not valid
* @param {object[]} rules - CORSRule collection parsed from xml to be validated
* @param {string[]} [rules[].ID] - optional id to identify rule
* @param {string[]} rules[].AllowedMethod - methods allowed for CORS
* @param {string[]} rules[].AllowedOrigin - origins allowed for CORS
* @param {string[]} [rules[].AllowedHeader] - headers allowed in an OPTIONS
* request via the Access-Control-Request-Headers header
* @param {string[]} [rules[].MaxAgeSeconds] - seconds browsers should cache
* OPTIONS response
* @param {string[]} [rules[].ExposeHeader] - headers exposed to applications
* @return {(Error|object)} - return cors object on success; error on failure
*/
function _validateCorsXml(rules) {
    const cors = [];
    let result;

    if (rules.length > 100) {
        return errors.InvalidRequest
            .customizeDescription(customizedErrs.numberRules);
    }
    for (let i = 0; i < rules.length; i++) {
        const rule = rules[i];
        const corsRule = {};

        result = _validator.validateOriginAndMethodExist(rule.AllowedMethod,
            rule.AllowedOrigin);
        if (result instanceof Error) {
            return result;
        }

        result = _validator.validateMethods(rule.AllowedMethod);
        if (result instanceof Error) {
            return result;
        }
        corsRule.allowedMethods = rule.AllowedMethod;

        result = _validator.validateAllowedOriginsOrHeaders(rule.AllowedOrigin,
            'AllowedOrigin');
        if (result instanceof Error) {
            return result;
        }
        corsRule.allowedOrigins = rule.AllowedOrigin;

        result = _validator.validateID(rule.ID);
        if (result instanceof Error) {
            return result;
        } else if (result) {
            corsRule.id = rule.ID[0];
        }

        result = _validator.validateAllowedHeaders(rule.AllowedHeader);
        if (result instanceof Error) {
            return result;
        } else if (result) {
            corsRule.allowedHeaders = rule.AllowedHeader;
        }

        result = _validator.validateMaxAgeSeconds(rule.MaxAgeSeconds);
        if (result instanceof Error) {
            return result;
        } else if (result) {
            corsRule.maxAgeSeconds = result;
        }

        result = _validator.validateExposeHeaders(rule.ExposeHeader);
        if (result instanceof Error) {
            return result;
        } else if (result) {
            corsRule.exposeHeaders = rule.ExposeHeader;
        }

        cors.push(corsRule);
    }
    return cors;
}

/** parseCorsXml - Parse and validate xml body, returning cors object on success
* @param {string} xml - xml body to parse and validate
* @param {object} log - Werelogs logger
* @param {function} cb - callback to server
* @return {undefined} - calls callback with cors object on success, error on
*   failure
*/
export function parseCorsXml(xml, log, cb) {
    parseString(xml, (err, result) => {
        if (err) {
            log.trace('xml parsing failed', {
                error: err,
                method: 'parseCorsXml',
            });
            log.debug('invalid xml', { xml });
            return cb(errors.MalformedXML);
        }

        if (!result || !result.CORSConfiguration ||
            !result.CORSConfiguration.CORSRule ||
            !Array.isArray(result.CORSConfiguration.CORSRule)) {
            const errMsg = 'Invalid cors configuration xml';
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        }

        const validationRes =
            _validateCorsXml(result.CORSConfiguration.CORSRule);
        if (validationRes instanceof Error) {
            log.debug('xml validation failed', {
                error: validationRes,
                method: '_validateCorsXml',
                xml,
            });
            return cb(validationRes);
        }
        // if no error, validation returns cors object
        return cb(null, validationRes);
    });
}
