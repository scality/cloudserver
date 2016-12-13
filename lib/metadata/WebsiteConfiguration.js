export class RoutingRule {
    /**
    * Represents a routing rule in a website configuration.
    * @constructor
    * @param {object} params - object containing redirect and condition objects
    * @param {object} params.redirect - specifies how to redirect requests
    * @param {string} [params.redirect.protocol] - protocol to use for redirect
    * @param {string} [params.redirect.hostName] - hostname to use for redirect
    * @param {string} [params.redirect.replaceKeyPrefixWith] - string to replace
    *   keyPrefixEquals specified in condition
    * @param {string} [params.redirect.replaceKeyWith] - string to replace key
    * @param {string} [params.redirect.httpRedirectCode] - http redirect code
    * @param {object} [params.condition] - specifies conditions for a redirect
    * @param {string} [params.condition.keyPrefixEquals] - key prefix that
    *   triggers a redirect
    * @param {string} [params.condition.httpErrorCodeReturnedEquals] - http code
    *   that triggers a redirect
    */
    constructor(params) {
        if (params) {
            this._redirect = params.redirect;
            this._condition = params.condition;
        }
    }

    /**
    * Return copy of rule as plain object
    * @return {object} rule;
    */
    getRuleObject() {
        const rule = {
            redirect: this._redirect,
            condition: this._condition,
        };
        return rule;
    }

    /**
    * Return the condition object
    * @return {object} condition;
    */
    getCondition() {
        return this._condition;
    }

    /**
    * Return the redirect object
    * @return {object} redirect;
    */
    getRedirect() {
        return this._redirect;
    }
}

export class WebsiteConfiguration {
    /**
    * Object that represents website configuration
    * @constructor
    * @param {object} params - object containing params to construct Object
    * @param {string} params.indexDocument - key for index document object
    *   required when redirectAllRequestsTo is undefined
    * @param {string} [params.errorDocument] - key for error document object
    * @param {object} params.redirectAllRequestsTo - object containing info
    *   about how to redirect all requests
    * @param {string} params.redirectAllRequestsTo.hostName - hostName to use
    *   when redirecting all requests
    * @param {string} [params.redirectAllRequestsTo.protocol] - protocol to use
    *   when redirecting all requests ('http' or 'https')
    * @param {(RoutingRule[]|object[])} params.routingRules - array of Routing
    *   Rule instances or plain routing rule objects to cast as RoutingRule's
    */
    constructor(params) {
        if (params) {
            this._indexDocument = params.indexDocument;
            this._errorDocument = params.errorDocument;
            this._redirectAllRequestsTo = params.redirectAllRequestsTo;
            this.setRoutingRules(params.routingRules);
        }
    }

    /**
    * Return plain object with configuration info
    * @return {object} - Object copy of class instance
    */
    getConfig() {
        const websiteConfig = {
            indexDocument: this._indexDocument,
            errorDocument: this._errorDocument,
            redirectAllRequestsTo: this._redirectAllRequestsTo,
        };
        if (this._routingRules) {
            websiteConfig.routingRules =
            this._routingRules.map(rule => rule.getRuleObject());
        }
        return websiteConfig;
    }

    /**
    * Set the redirectAllRequestsTo
    * @param {object} obj - object to set as redirectAllRequestsTo
    * @param {string} obj.hostName - hostname for redirecting all requests
    * @param {object} [obj.protocol] - protocol for redirecting all requests
    * @return {undefined};
    */
    setRedirectAllRequestsTo(obj) {
        this._redirectAllRequestsTo = obj;
    }

    /**
    * Return the redirectAllRequestsTo object
    * @return {object} redirectAllRequestsTo;
    */
    getRedirectAllRequestsTo() {
        return this._redirectAllRequestsTo;
    }

    /**
    * Set the index document object name
    * @param {string} suffix - index document object key
    * @return {undefined};
    */
    setIndexDocument(suffix) {
        this._indexDocument = suffix;
    }

    /**
     * Get the index document object name
     * @return {string} indexDocument
     */
    getIndexDocument() {
        return this._indexDocument;
    }

    /**
     * Set the error document object name
     * @param {string} key - error document object key
     * @return {undefined};
     */
    setErrorDocument(key) {
        this._errorDocument = key;
    }

    /**
     * Get the error document object name
     * @return {string} errorDocument
     */
    getErrorDocument() {
        return this._errorDocument;
    }

    /**
    * Set the whole RoutingRules array
    * @param {array} array - array to set as instance's RoutingRules
    * @return {undefined};
    */
    setRoutingRules(array) {
        if (array) {
            this._routingRules = array.map(rule => {
                if (rule instanceof RoutingRule) {
                    return rule;
                }
                return new RoutingRule(rule);
            });
        }
    }

    /**
     * Add a RoutingRule instance to routingRules array
     * @param {object} obj - rule to add to array
     * @return {undefined};
     */
    addRoutingRule(obj) {
        if (!this._routingRules) {
            this._routingRules = [];
        }
        if (obj && obj instanceof RoutingRule) {
            this._routingRules.push(obj);
        } else if (obj) {
            this._routingRules.push(new RoutingRule(obj));
        }
    }

    /**
     * Get routing rules
     * @return {RoutingRule[]} - array of RoutingRule instances
     */
    getRoutingRules() {
        return this._routingRules;
    }
}
