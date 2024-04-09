const { default: ScubaClient } = require('scubaclient');
const { config } = require('../Config');
const { externalBackendHealthCheckInterval } = require('../../constants');

class ScubaClientImpl extends ScubaClient {
    constructor(config) {
        super(config.scuba);
        this.enabled = false;
        this._healthCheckTimer = null;
        this._log = null;

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
        return this.healthCheck().then(() => {
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
}

const ScubaClientInstance = new ScubaClientImpl(config);

module.exports = {
    ScubaClientInstance,
    ScubaClientImpl,
};
