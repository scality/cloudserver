const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const { reportHandler } = require('./reportHandler');
const { monitoringHandler } = require('./monitoringHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
    monitoring: monitoringHandler,
    metadata: routeMetadata,
};

module.exports = {
    internalHandlers,
};
