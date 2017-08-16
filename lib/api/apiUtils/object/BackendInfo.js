const { config } = require('../../../Config');
const escapeForXml = require('arsenal').s3middleware.escapeForXml;

class BackendInfo {
    /**
    * Represents the info necessary to evaluate which data backend to use
    * on a data put call.
    * @constructor
    * @param {string | undefined} objectLocationConstraint - location constraint
    * for object based on user meta header
    * @param {string | undefined } bucketLocationConstraint - location
    * constraint for bucket based on bucket metadata
    * @param {string} requestEndpoint - endpoint to which request was made
    */
    constructor(objectLocationConstraint, bucketLocationConstraint,
        requestEndpoint) {
        this._objectLocationConstraint = objectLocationConstraint;
        this._bucketLocationConstraint = bucketLocationConstraint;
        this._requestEndpoint = requestEndpoint;
        return this;
    }

    /**
     * validate proposed location constraint against config
     * @param {string | undefined} locationConstraint - value of user
     * metadata location constraint header or bucket location constraint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if valid, false if not
     */
    static isValidLocationConstraint(locationConstraint, log) {
        if (Object.keys(config.locationConstraints).
            indexOf(locationConstraint) < 0) {
            log.trace('proposed locationConstraint is invalid',
                { locationConstraint });
            return false;
        }
        return true;
    }

    /**
     * validate that request endpoint is listed in the restEndpoint config
     * @param {string} requestEndpoint - request endpoint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if present, false if not
     */
    static isRequestEndpointPresent(requestEndpoint, log) {
        if (Object.keys(config.restEndpoints).indexOf(requestEndpoint) < 0) {
            log.trace('requestEndpoint does not match config restEndpoints',
              { requestEndpoint });
            return false;
        }
        return true;
    }

    /**
     * validate that locationConstraint for request Endpoint matches
     * one config locationConstraint
     * @param {string} requestEndpoint - request endpoint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if matches, false if not
     */
    static isRequestEndpointValueValid(requestEndpoint, log) {
        if (Object.keys(config.locationConstraints).indexOf(config
            .restEndpoints[requestEndpoint]) < 0) {
            log.trace('the default locationConstraint for request' +
                'Endpoint does not match any config locationConstraint',
                { requestEndpoint });
            return false;
        }
        return true;
    }

    /**
     * validate that s3 server is running with a file or memory backend
     * @param {string} requestEndpoint - request endpoint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if running with file/mem backend, false if not
     */
    static isMemOrFileBackend(requestEndpoint, log) {
        if (config.backends.data === 'mem' ||
        config.backends.data === 'file') {
            log.trace('use data backend for the location', {
                dataBackend: config.backends.data,
                method: 'isMemOrFileBackend',
            });
            return true;
        }
        return false;
    }

    /**
     * validate requestEndpoint against config or mem/file data backend
     * - if there is no match for the request endpoint in the config
     * restEndpoints and data backend is set to mem or file we will use this
     * data backend for the location.
     * - if locationConstraint for request Endpoint does not match
     * any config locationConstraint, we will return an error
     * @param {string} requestEndpoint - request endpoint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if valid, false if not
     */
    static isValidRequestEndpointOrBackend(requestEndpoint, log) {
        if (!BackendInfo.isRequestEndpointPresent(requestEndpoint, log)) {
            return BackendInfo.isMemOrFileBackend(requestEndpoint, log);
        }
        return BackendInfo.isRequestEndpointValueValid(requestEndpoint, log);
    }

    /**
     * validate controlling BackendInfo Parameter
     * @param {string | undefined} objectLocationConstraint - value of user
     * metadata location constraint header
     * @param {string | null} bucketLocationConstraint - location
     * constraint from bucket metadata
     * @param {string} requestEndpoint - endpoint of request
     * @param {object} log - werelogs logger
     * @return {object} - location contraint validity
     */
    static controllingBackendParam(objectLocationConstraint,
        bucketLocationConstraint, requestEndpoint, log) {
        if (objectLocationConstraint) {
            if (BackendInfo.isValidLocationConstraint(objectLocationConstraint,
                log)) {
                log.trace('objectLocationConstraint is valid');
                return { isValid: true };
            }
            log.trace('objectLocationConstraint is invalid');
            return { isValid: false, description: 'Object Location Error - ' +
            `Your object location "${escapeForXml(objectLocationConstraint)}"` +
            'is not  in your location config - Please update.' };
        }
        if (bucketLocationConstraint) {
            if (BackendInfo.isValidLocationConstraint(bucketLocationConstraint,
                log)) {
                log.trace('bucketLocationConstraint is valid');
                return { isValid: true };
            }
            log.trace('bucketLocationConstraint is invalid');
            return { isValid: false, description: 'Bucket Location Error - ' +
            `Your bucket location "${escapeForXml(bucketLocationConstraint)}"` +
            ' is not in your location config - Please update.' };
        }
        if (!BackendInfo.isValidRequestEndpointOrBackend(requestEndpoint,
          log)) {
            return { isValid: false, description: 'Endpoint Location Error - ' +
            `Your endpoint "${requestEndpoint}" is not in restEndpoints ` +
            'in your config OR the default location constraint for request ' +
            `endpoint "${escapeForXml(requestEndpoint)}" does not ` +
            'match any config locationConstraint - Please update.' };
        }
        return { isValid: true };
    }

    /**
    * Return objectLocationConstraint
    * @return {string | undefined} objectLocationConstraint;
    */
    getObjectLocationConstraint() {
        return this._objectLocationConstraint;
    }

    /**
    * Return bucketLocationConstraint
    * @return {string | undefined} bucketLocationConstraint;
    */
    getBucketLocationConstraint() {
        return this._bucketLocationConstraint;
    }

    /**
    * Return requestEndpoint
    * @return {string} requestEndpoint;
    */
    getRequestEndpoint() {
        return this._requestEndpoint;
    }

    /**
    * Return locationConstraint that should be used with put request
    * Order of priority is:
    * (1) objectLocationConstraint,
    * (2) bucketLocationConstraint,
    * (3) default locationConstraint for requestEndpoint  if requestEndpoint
    *     is listed in restEndpoints in config.json
    * (4) default data backend
    * @return {string} locationConstraint;
    */
    getControllingLocationConstraint() {
        const objectLC = this.getObjectLocationConstraint();
        const bucketLC = this.getBucketLocationConstraint();
        const reqEndpoint = this.getRequestEndpoint();
        if (objectLC) {
            return objectLC;
        }
        if (bucketLC) {
            return bucketLC;
        }
        if (config.restEndpoints[reqEndpoint]) {
            return config.restEndpoints[reqEndpoint];
        }
        return config.backends.data;
    }
}

module.exports = {
    BackendInfo,
};
