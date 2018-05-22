const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const { reportHandler } = require('./reportHandler');
const { monitoringHandler } = require('./monitoringHandler');
const routeUtapi = require('../routes/routeUtapi');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
    monitoring: monitoringHandler,
    utapi: routeUtapi,
};

module.exports = {
    internalHandlers,
};
