const { healthcheckHandler } = require('./healthcheckHandler');
const routeBackbeat = require('../routes/routeBackbeat');

const internalHandlers = {
    healthcheck: healthcheckHandler,
    backbeat: routeBackbeat,
};

module.exports = {
    internalHandlers,
};
