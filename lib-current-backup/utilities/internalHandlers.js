const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const routeWorkflowEngineOperator =
      require('../routes/routeWorkflowEngineOperator');
const { reportHandler } = require('./reportHandler');
const routeVeeam = require('../routes/routeVeeam').routeVeeam;

const internalHandlers = {
    backbeat: routeBackbeat,
    report: reportHandler,
    metadata: routeMetadata,
    'workflow-engine-operator': routeWorkflowEngineOperator,
    veeam: routeVeeam,
};

module.exports = {
    internalHandlers,
};
