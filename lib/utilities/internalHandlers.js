const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const { reportHandler } = require('./reportHandler');
const { monitoringHandler } = require('./monitoringHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
    monitoring: monitoringHandler,
};

module.exports = {
    internalHandlers,
};
