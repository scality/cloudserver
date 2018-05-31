const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const routeMetadata = require('../routes/routeMetadata');
const { reportHandler } = require('./reportHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
    metadata: routeMetadata,
};

module.exports = {
    internalHandlers,
};
