const { default: ScubaClient } = require('scubaclient');
const { externalBackendHealthCheckInterval } = require('../../../constants');
const monitoring = require('../../utilities/monitoringHandler');

class ScubaClientImpl extends ScubaClient {
    constructor(config) {
        super(config.scuba);
        this.enabled = false;
        this.maxStaleness = config.quota.maxStaleness;
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

    async _healthCheck() {
        try {
            const data = await this.healthCheck();
            if (data?.date) {
                const date = new Date(data.date);
                if (Date.now() - date.getTime() > this.maxStaleness) {
                    throw new Error('Data is stale, disabling quotas');
                }
            }
            if (!this.enabled) {
                this._log.info('Scuba health check passed, enabling quotas');
            }
            monitoring.utilizationServiceAvailable.set(1);
            this.enabled = true;
        } catch (err) {
            if (this.enabled) {
                this._log.warn('Scuba health check failed, disabling quotas', {
                    err: err.name,
                    description: err.message,
                });
            }
            monitoring.utilizationServiceAvailable.set(0);
            this.enabled = false;
        }
    }

    periodicHealthCheck() {
        if (this._healthCheckTimer) {
            clearInterval(this._healthCheckTimer);
        }
        this._healthCheck();
        this._healthCheckTimer = setInterval(
            () => this._healthCheck(),
            Number(process.env.SCUBA_HEALTHCHECK_FREQUENCY) || externalBackendHealthCheckInterval,
        );
    }

    async getUtilizationMetrics(metricsClass, resourceName, options, body, log) {
        const requestStartTime = process.hrtime.bigint();
        const reqUids = log?.getSerializedUids?.();
        const mergedOptions = reqUids
            ? { ...options, headers: { ...options?.headers, 'X-Scal-Request-Uids': reqUids } }
            : options;
        let code = 200;
        try {
            return await super.getLatestMetrics(metricsClass, resourceName, mergedOptions, body);
        } catch (err) {
            code = err.statusCode || 500;
            throw err;
        } finally {
            const responseTimeInNs = Number(process.hrtime.bigint() - requestStartTime);
            monitoring.utilizationMetricsRetrievalDuration
                .labels({ code, class: metricsClass })
                .observe(responseTimeInNs / 1e9);
        }
    }
}

module.exports = {
    ScubaClientImpl,
};
