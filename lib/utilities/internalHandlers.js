const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const routeWorkflowEngineOperator =
      require('../routes/routeWorkflowEngineOperator');
const { reportHandler } = require('./reportHandler');

const internalHandlers = {
    backbeat: routeBackbeat,
    report: reportHandler,
    metadata: routeMetadata,
    'workflow-engine-operator': routeWorkflowEngineOperator,
};

module.exports = {
    internalHandlers,
};
