const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const routeWorkflowEngineOperator =
      require('../routes/routeWorkflowEngineOperator');
const { reportHandler } = require('./reportHandler');
const { monitoringHandler } = require('./monitoringHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
    monitoring: monitoringHandler,
    metadata: routeMetadata,
    'workflow-engine-operator': routeWorkflowEngineOperator,
};

module.exports = {
    internalHandlers,
};
