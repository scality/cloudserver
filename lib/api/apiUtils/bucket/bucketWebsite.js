const { parseString } = require('xml2js');
const { errors, s3middleware } = require('arsenal');

const escapeForXml = s3middleware.escapeForXml;
const { WebsiteConfiguration } =
  require('arsenal').models.WebsiteConfiguration;

/*
   Format of xml request:

   <WebsiteConfiguration xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
     <IndexDocument>
       <Suffix>index.html</Suffix>
     </IndexDocument>
     <ErrorDocument>
       <Key>Error.html</Key>
     </ErrorDocument>

     <RoutingRules>
       <RoutingRule>
       <Condition>
         <KeyPrefixEquals>docs/</KeyPrefixEquals>
       </Condition>
       <Redirect>
         <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
       </Redirect>
       </RoutingRule>
       ...
    </RoutingRules>
   </WebsiteConfiguration>
   */


// Key names of redirect object values to check if are valid strings
const redirectValuesToCheck = ['HostName', 'ReplaceKeyPrefixWith',
    'ReplaceKeyWith'];

/** Helper function for validating format of parsed xml element
* @param {array} elem - element to check
* @return {boolean} true / false - elem meets expected format
*/
function _isValidElem(elem) {
    return (Array.isArray(elem) && elem.length === 1);
}

/** Check if parsed xml element contains a specified child element
* @param {array} parent - represents xml element to check for child element
* @param {(string|string[])} requiredElem - name of child element(s)
* @param {object} [options] - specify additional options
* @param {boolean} [isList] - indicates if parent is list of children elements,
* used only in conjunction a singular requiredElem argument
* @param {boolean} [checkForAll] - return true only if parent element contains
* all children elements specified in requiredElem; by default, returns true if
* parent element contains at least one
* @param {boolean} [validateParent] - validate format of parent element
* @return {boolean} true / false - if parsed xml element contains child
*/
function xmlContainsElem(parent, requiredElem, options) {
    // Non-top level xml is parsed into object in the following manner.

    // Example: <Parent><requiredElem>value</requiredElem>
    //          <anotherElem>value2</anotherElem></Parent>
    // Result: { Parent: [{ requiredElem: ["value"],
    //            anotherElem: ["value2"] }] }

    // Example for xml list:
    //  <ParentList>
    //      <requiredElem>value</requiredElem>
    //      <requiredElem>value</requiredElem>
    //      <requiredElem>value</requiredElem>
    //  </ParentList>
    // Result: { ParentList: [{ requiredElem: ['value', 'value', 'value'] }] }

    const isList = options ? options.isList : false;
    const checkForAll = options ? options.checkForAll : false;
    // true by default, validateParent only designated as false when
    // parent was validated in previous check
    const validateParent = (options && options.validateParent !== undefined) ?
        options.validateParent : true;

    if (validateParent && !_isValidElem(parent)) {
        return false;
    }
    if (Array.isArray(requiredElem)) {
        if (checkForAll) {
            return requiredElem.every(elem => _isValidElem(parent[0][elem]));
        }
        return requiredElem.some(elem => _isValidElem(parent[0][elem]));
    }
    if (isList) {
        if (!Array.isArray(parent[0][requiredElem]) ||
        parent[0][requiredElem].length === 0) {
            return false;
        }
    } else {
        return _isValidElem(parent[0][requiredElem]);
    }

    return true;
}


/** Validate XML, returning an error if any part is not valid
* @param {object} parsingResult - object parsed from xml to be validated
* @param {object[]} parsingResult.IndexDocument -
*   Required if RedirectAllRequestsTo is not defined
* @param {string[]} parsingResult.IndexDocument[].Suffix -
*   Key that specifies object in bucket to serve as index
* @param {object[]=} parsingResult.ErrorDocument - Optional
* @param {string[]} parsingResult.ErrorDocument[].Key -
*   Key that specifies object in bucket to serve as error document.
* @param {object[]} parsingResult.RedirectAllRequestsTo -
*   Contains fields to specify how to redirect all requests to bucket.
* @param {string[]} parsingResult.RedirectAllRequestsTo[].HostName -
*   Hostname to use when redirecting all requests.
* @param {string[]=} parsingResult.RedirectAllRequestsTo[].Protocol -
*   Optional, protocol to use when redirecting all request ('http' or 'https')
* @param {object[]=} parsingResult.RoutingRules -
*   Contains list of rules for redirecting specific requests.
* @param {object[]} parsingResult.RoutingRules[].RoutingRule -
*   Contains redirect and condition information for specific redirects.
* @param {object[]} parsingResult.RoutingRules[].RoutingRule[].Redirect -
*   Contains information for how to redirect, required for a RoutingRule.
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[].Redirect[]
*   .Protocol - Protocol to use for a specific redirect.
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[].Redirect[]
*   .HostName - Hostname to use in a specific redirect.
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[].Redirect[]
*   .ReplaceKeyPrefixWith - What to replace a key prefix specified in a
*   Condition with in a redirect
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[].Redirect[]
*   .ReplaceKeyWith - What to replace the object key with in a redirect
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[].Redirect[]
*   .HttpRedirectCode - Http code (301-399) to use in redirect.
* @param {object[]=} parsingResult.RoutingRules[].RoutingRule[].Condition
    - Optional, contains fields for conditions to make a specific redirect
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[]
*   .Condition[].KeyPrefixEquals - Specify the prefix to match for a redirect
* @param {string[]=} parsingResult.RoutingRules[].RoutingRule[]
*   .Condition[].HttpErrorCodeReturnedEquals - Error code to match for redirect
* @return {(Error|WebsiteConfiguration)} return WebsiteConfiguration on success;
*   otherwise return error
*/
function _validateWebsiteConfigXml(parsingResult) {
    const websiteConfig = new WebsiteConfiguration();
    let errMsg;

    function _isValidString(value) {
        return (typeof value === 'string' && value !== '');
    }

    if (!parsingResult.IndexDocument && !parsingResult.RedirectAllRequestsTo) {
        errMsg = 'Value for IndexDocument Suffix must be provided if ' +
        'RedirectAllRequestsTo is empty';
        return errors.InvalidArgument.customizeDescription(errMsg);
    }

    if (parsingResult.RedirectAllRequestsTo) {
        const parent = parsingResult.RedirectAllRequestsTo;
        const redirectAllObj = {};
        if (parsingResult.IndexDocument || parsingResult.ErrorDocument ||
        parsingResult.RoutingRules) {
            errMsg = 'RedirectAllRequestsTo cannot be provided in ' +
            'conjunction with other Routing Rules.';
            return errors.InvalidRequest.customizeDescription(errMsg);
        }
        if (!xmlContainsElem(parent, 'HostName')) {
            errMsg = 'RedirectAllRequestsTo not well-formed';
            return errors.MalformedXML.customizeDescription(errMsg);
        }
        if (!_isValidString(parent[0].HostName[0])) {
            errMsg = 'Valid HostName required in RedirectAllRequestsTo';
            return errors.InvalidRequest.customizeDescription(errMsg);
        }
        redirectAllObj.hostName = parent[0].HostName[0];
        if (xmlContainsElem(parent, 'Protocol', { validateParent: false })) {
            if (parent[0].Protocol[0] !== 'http' &&
            parent[0].Protocol[0] !== 'https') {
                errMsg = 'Invalid protocol, protocol can be http or https. ' +
                'If not defined, the protocol will be selected automatically.';
                return errors.InvalidRequest.customizeDescription(errMsg);
            }
            redirectAllObj.protocol = parent[0].Protocol[0];
        }
        websiteConfig.setRedirectAllRequestsTo(redirectAllObj);
    }

    if (parsingResult.IndexDocument) {
        const parent = parsingResult.IndexDocument;
        if (!xmlContainsElem(parent, 'Suffix')) {
            errMsg = 'IndexDocument is not well-formed';
            return errors.MalformedXML.customizeDescription(errMsg);
        } else if (!_isValidString(parent[0].Suffix[0])
        || parent[0].Suffix[0].indexOf('/') !== -1) {
            errMsg = 'IndexDocument Suffix is not well-formed';
            return errors.InvalidArgument.customizeDescription(errMsg);
        }
        websiteConfig.setIndexDocument(parent[0].Suffix[0]);
    }

    if (parsingResult.ErrorDocument) {
        const parent = parsingResult.ErrorDocument;
        if (!xmlContainsElem(parent, 'Key')) {
            errMsg = 'ErrorDocument is not well-formed';
            return errors.MalformedXML.customizeDescription(errMsg);
        }
        if (!_isValidString(parent[0].Key[0])) {
            errMsg = 'ErrorDocument Key is not well-formed';
            return errors.InvalidArgument.customizeDescription(errMsg);
        }
        websiteConfig.setErrorDocument(parent[0].Key[0]);
    }

    if (parsingResult.RoutingRules) {
        const parent = parsingResult.RoutingRules;
        if (!xmlContainsElem(parent, 'RoutingRule', { isList: true })) {
            errMsg = 'RoutingRules is not well-formed';
            return errors.MalformedXML.customizeDescription(errMsg);
        }
        for (let i = 0; i < parent[0].RoutingRule.length; i++) {
            const rule = parent[0].RoutingRule[i];
            const ruleObj = { redirect: {} };
            if (!_isValidElem(rule.Redirect)) {
                errMsg = 'RoutingRule requires Redirect, which is ' +
                'missing or not well-formed';
                return errors.MalformedXML.customizeDescription(errMsg);
            }
            // Looks like AWS doesn't actually make this check, but AWS
            // documentation specifies at least one of the following elements
            // must be in a Redirect rule. We also need at least one of the
            // elements to know how to implement a redirect for a rule.
            // http://docs.aws.amazon.com/AmazonS3/latest/API/
            // RESTBucketPUTwebsite.html
            if (!xmlContainsElem(rule.Redirect, ['Protocol', 'HostName',
            'ReplaceKeyPrefixWith', 'ReplaceKeyWith', 'HttpRedirectCode'],
            { validateParent: false })) {
                errMsg = 'Redirect must contain at least one of ' +
                'following: Protocol, HostName, ReplaceKeyPrefixWith, ' +
                'ReplaceKeyWith, or HttpRedirectCode element';
                return errors.MalformedXML.customizeDescription(errMsg);
            }
            if (rule.Redirect[0].Protocol) {
                if (!_isValidElem(rule.Redirect[0].Protocol) ||
                (rule.Redirect[0].Protocol[0] !== 'http' &&
                rule.Redirect[0].Protocol[0] !== 'https')) {
                    errMsg = 'Invalid protocol, protocol can be http or ' +
                    'https. If not defined, the protocol will be selected ' +
                    'automatically.';
                    return errors.InvalidRequest.customizeDescription(errMsg);
                }
                ruleObj.redirect.protocol = rule.Redirect[0].Protocol[0];
            }
            if (rule.Redirect[0].HttpRedirectCode) {
                errMsg = 'The provided HTTP redirect code is not valid. ' +
                'It should be a string containing a number.';
                if (!_isValidElem(rule.Redirect[0].HttpRedirectCode)) {
                    return errors.MalformedXML.customizeDescription(errMsg);
                }
                const code = parseInt(rule.Redirect[0].HttpRedirectCode[0], 10);
                if (isNaN(code)) {
                    return errors.MalformedXML.customizeDescription(errMsg);
                }
                if (!(code > 300 && code < 400)) {
                    errMsg = `The provided HTTP redirect code (${code}) is ` +
                    'not valid. Valid codes are 3XX except 300';
                    return errors.InvalidRequest.customizeDescription(errMsg);
                }
                ruleObj.redirect.httpRedirectCode = code;
            }
            for (let j = 0; j < redirectValuesToCheck.length; j++) {
                const elemName = redirectValuesToCheck[j];
                const elem = rule.Redirect[0][elemName];
                if (elem) {
                    if (!_isValidElem(elem) || !_isValidString(elem[0])) {
                        errMsg = `Redirect ${elem} is not well-formed`;
                        return errors.InvalidArgument
                            .customizeDescription(errMsg);
                    }
                    ruleObj.redirect[`${elemName.charAt(0).toLowerCase()}` +
                    `${elemName.slice(1)}`] = elem[0];
                }
            }
            if (xmlContainsElem(rule.Redirect, ['ReplaceKeyPrefixWith',
            'ReplaceKeyWith'], { validateParent: false, checkForAll: true })) {
                errMsg = 'Redirect must not contain both ReplaceKeyWith ' +
                'and ReplaceKeyPrefixWith';
                return errors.InvalidRequest.customizeDescription(errMsg);
            }
            if (Array.isArray(rule.Condition) && rule.Condition.length === 1) {
                ruleObj.condition = {};
                if (!xmlContainsElem(rule.Condition, ['KeyPrefixEquals',
                'HttpErrorCodeReturnedEquals'])) {
                    errMsg = 'Condition is not well-formed. ' +
                    'Condition should contain valid KeyPrefixEquals or ' +
                    'HttpErrorCodeReturnEquals element.';
                    return errors.InvalidRequest.customizeDescription(errMsg);
                }
                if (rule.Condition[0].KeyPrefixEquals) {
                    const keyPrefixEquals = rule.Condition[0].KeyPrefixEquals;
                    if (!_isValidElem(keyPrefixEquals) ||
                    !_isValidString(keyPrefixEquals[0])) {
                        errMsg = 'Condition KeyPrefixEquals is not well-formed';
                        return errors.InvalidArgument
                            .customizeDescription(errMsg);
                    }
                    ruleObj.condition.keyPrefixEquals = keyPrefixEquals[0];
                }
                if (rule.Condition[0].HttpErrorCodeReturnedEquals) {
                    errMsg = 'The provided HTTP error code is not valid. ' +
                    'It should be a string containing a number.';
                    if (!_isValidElem(rule.Condition[0]
                        .HttpErrorCodeReturnedEquals)) {
                        return errors.MalformedXML.customizeDescription(errMsg);
                    }
                    const code = parseInt(rule.Condition[0]
                        .HttpErrorCodeReturnedEquals[0], 10);
                    if (isNaN(code)) {
                        return errors.MalformedXML.customizeDescription(errMsg);
                    }
                    if (!(code > 399 && code < 600)) {
                        errMsg = `The provided HTTP error code (${code}) is ` +
                        'not valid. Valid codes are 4XX or 5XX.';
                        return errors.InvalidRequest
                            .customizeDescription(errMsg);
                    }
                    ruleObj.condition.httpErrorCodeReturnedEquals = code;
                }
            }
            websiteConfig.addRoutingRule(ruleObj);
        }
    }
    return websiteConfig;
}

function parseWebsiteConfigXml(xml, log, cb) {
    parseString(xml, (err, result) => {
        if (err) {
            log.trace('xml parsing failed', {
                error: err,
                method: 'parseWebsiteConfigXml',
            });
            log.debug('invalid xml', { xmlObj: xml });
            return cb(errors.MalformedXML);
        }

        if (!result || !result.WebsiteConfiguration) {
            const errMsg = 'Invalid website configuration xml';
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        }

        const validationRes =
            _validateWebsiteConfigXml(result.WebsiteConfiguration);
        if (validationRes instanceof Error) {
            log.debug('xml validation failed', {
                error: validationRes,
                method: '_validateWebsiteConfigXml',
                xml,
            });
            return cb(validationRes);
        }
        // if no error, validation returns instance of WebsiteConfiguration
        log.trace('website configuration', { validationRes });
        return cb(null, validationRes);
    });
}

function convertToXml(config) {
    const xml = [];
    const indexDocument = config.getIndexDocument();
    const errorDocument = config.getErrorDocument();
    const redirectAllRequestsTo = config.getRedirectAllRequestsTo();
    const routingRules = config.getRoutingRules();

    function _pushChildren(obj) {
        Object.keys(obj).forEach(element => {
            const xmlElem = `${element.charAt(0).toUpperCase()}` +
                `${element.slice(1)}`;
            xml.push(`<${xmlElem}>${escapeForXml(obj[element])}</${xmlElem}>`);
        });
    }
    xml.push('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<WebsiteConfiguration xmlns=' +
        '"http://s3.amazonaws.com/doc/2006-03-01/">');
    if (indexDocument) {
        xml.push('<IndexDocument>',
        `<Suffix>${escapeForXml(indexDocument)}</Suffix>`,
        '</IndexDocument>');
    }
    if (errorDocument) {
        xml.push('<ErrorDocument>',
        `<Key>${escapeForXml(errorDocument)}</Key>`,
        '</ErrorDocument>');
    }
    if (redirectAllRequestsTo) {
        xml.push('<RedirectAllRequestsTo>');
        if (redirectAllRequestsTo.hostName) {
            xml.push('<HostName>',
            `${escapeForXml(redirectAllRequestsTo.hostName)}`,
            '</HostName>');
        }
        if (redirectAllRequestsTo.protocol) {
            xml.push('<Protocol>',
            `${redirectAllRequestsTo.protocol}`,
            '</Protocol>');
        }
        xml.push('</RedirectAllRequestsTo>');
    }
    if (routingRules) {
        xml.push('<RoutingRules>');
        routingRules.forEach(rule => {
            const condition = rule.getCondition();
            const redirect = rule.getRedirect();
            xml.push('<RoutingRule>');
            if (condition) {
                xml.push('<Condition>');
                _pushChildren(condition);
                xml.push('</Condition>');
            }
            if (redirect) {
                xml.push('<Redirect>');
                _pushChildren(redirect);
                xml.push('</Redirect>');
            }
            xml.push('</RoutingRule>');
        });
        xml.push('</RoutingRules>');
    }
    xml.push('</WebsiteConfiguration>');
    return xml.join('');
}

module.exports = {
    xmlContainsElem,
    parseWebsiteConfigXml,
    convertToXml,
};
