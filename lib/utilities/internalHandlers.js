const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const routeWorkflowEngineOperator =
      require('../routes/routeWorkflowEngineOperator');
const { reportHandler } = require('./reportHandler');
const routeVeeam = require('../routes/routeVeeam').routeVeeam;
const { healthcheckHandler } = require('./healthcheckHandler');
const { config } = require('../Config');

const internalHandlers = {
    backbeat: routeBackbeat,
    report: reportHandler,
    metadata: routeMetadata,
};

if (config.workflowEngineOperator) {
    internalHandlers['workflow-engine-operator'] = routeWorkflowEngineOperator;
}

if (config.enableVeeamRoute) {
    internalHandlers.veeam = routeVeeam;
}

if (config.healthChecks.enableInternalRoute) {
    // CLDSRV-740: S3C with the healthcheck on s3 port needs the internal route
    internalHandlers.healthcheck = function s3InternalHealthcheckHandler(clientIP, req, res, log, statsClient) {
        const deep = req.url === '/_/healthcheck/deep';
        return healthcheckHandler(clientIP, req, res, log, statsClient, deep);
    };
}

module.exports = {
    internalHandlers,
};
