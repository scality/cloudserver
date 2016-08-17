'use strict'; // eslint-disable-line strict

require('babel-core/register')({
    ignore: filename =>
        !(filename.startsWith(`${__dirname}/constants.js`) ||
        filename.startsWith(`${__dirname}/lib/`) ||
        filename.startsWith(`${__dirname}/tests/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/lib/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/utils/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/router/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/responses/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/validators/`) ||
        filename.startsWith(`${__dirname}/node_modules/utapi/handlers/`)),
});
require('./lib/utapi.js').default();
