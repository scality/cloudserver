const util = require('util');
const { default: ScubaClient } = require('scubaclient');
const { externalBackendHealthCheckInterval } = require('../../../constants');

class ScubaClientImpl extends ScubaClient {
    constructor(config) {
        super(config.scuba);
        this.enabled = false;
        this.maxStaleness = config.quota.maxStaleness;
        this._healthCheckTimer = null;
        this._log = null;
        this._getLatestMetricsCallback = util.callbackify(this.getLatestMetrics);

        if (config.scuba) {
            this.enabled = true;
        } else {
            this.enabled = false;
        }
    }

    setup(log) {
        this._log = log;
        if (this.enabled) {
            this.periodicHealthCheck();
        }
    }

    _healthCheck() {
        return this.healthCheck().then(data => {
            if (data?.date) {
                const date = new Date(data.date);
                if (Date.now() - date.getTime() > this.maxStaleness) {
                    throw new Error('Data is stale, disabling quotas');
                }
            }
            if (!this.enabled) {
                this._log.info('Scuba health check passed, enabling quotas');
            }
            this.enabled = true;
        }).catch(err => {
            if (this.enabled) {
                this._log.warn('Scuba health check failed, disabling quotas', {
                    err: err.name,
                    description: err.message,
                });
            }
            this.enabled = false;
        });
    }

    periodicHealthCheck() {
        if (this._healthCheckTimer) {
            clearInterval(this._healthCheckTimer);
        }
        this._healthCheck();
        this._healthCheckTimer = setInterval(async () => {
            this._healthCheck();
        }, Number(process.env.SCUBA_HEALTHCHECK_FREQUENCY)
            || externalBackendHealthCheckInterval);
    }

    getUtilizationMetrics(metricsClass, resourceName, options, body, callback) {
        return this._getLatestMetricsCallback(metricsClass, resourceName, options, body, callback);
    }
}

module.exports = {
    ScubaClientImpl,
};
