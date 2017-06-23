const { healthcheckHandler } = require('./healthcheckHandler');

const internalHandlers = {
    healthcheck: healthcheckHandler,
};

module.exports = {
    internalHandlers,
};
