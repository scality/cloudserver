const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');
const { reportHandler } = require('./reportHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
    report: reportHandler,
};

module.exports = {
    internalHandlers,
};
