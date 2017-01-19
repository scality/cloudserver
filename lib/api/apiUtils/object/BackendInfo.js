import config from '../../../Config';

export class BackendInfo {
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
        if (Object.keys(config.locationConstraints)
            .indexOf(locationConstraint) < 0) {
            log.trace('proposed locationConstraint is invalid',
                { locationConstraint });
            return false;
        }
        return true;
    }

    /**
     * validate requestEndpoint against config
     * if there is a mismatch between the request endpoint and what is in
     * the config, this could cause a problem for setting the backend location
     * for storing data
     * @param {string} requestEndpoint - request endpoint
     * @param {object} log - werelogs logger
     * @return {boolean} - true if valid, false if not
     */
    static isValidRequestEndpoint(requestEndpoint, log) {
        if (Object.keys(config.restEndpoints)
            .indexOf(requestEndpoint) < 0) {
            log.trace('requestEndpoint does not match config restEndpoints',
                { requestEndpoint });
            return false;
        }
        if (Object.keys(config.locationConstraints).indexOf(config
            .restEndpoints[requestEndpoint]) < 0) {
            log.trace('the default locationConstraint for requestEndpoint ' +
                'does not match any config locationConstraint',
                { requestEndpoint });
            return false;
        }
        return true;
    }

    /**
     * validate proposed BackendInfo parameters
     * @param {string | undefined} objectLocationConstraint - value of user
     * metadata location constraint header
     * @param {string | null} bucketLocationConstraint - location
     * constraint from bucket metadata
     * @param {string} requestEndpoint - endpoint of request
     * @param {object} log - werelogs logger
     * @return {boolean} - true if valid, false if not
     */
    static areValidBackendParameters(objectLocationConstraint,
        bucketLocationConstraint, requestEndpoint, log) {
        if (objectLocationConstraint &&
            !BackendInfo.isValidLocationConstraint(objectLocationConstraint,
                log)) {
            log.trace('objectLocationConstraint is invalid');
            return false;
        }
        if (bucketLocationConstraint &&
            !BackendInfo.isValidLocationConstraint(bucketLocationConstraint,
                log)) {
            log.trace('bucketLocationConstraint is invalid');
            return false;
        }
        if (!BackendInfo.isValidRequestEndpoint(requestEndpoint, log)) {
            return false;
        }
        return true;
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
    * (3) default locationConstraint for requestEndpoint
    * @return {string} locationConstraint;
    */
    getControllingLocationConstraint() {
        if (this._objectLocationConstraint) {
            return this.getObjectLocationConstraint();
        }
        if (this._bucketLocationConstraint) {
            return this.getBucketLocationConstraint();
        }
        return config.restEndpoints[this.getRequestEndpoint];
    }
}
