module.exports = function routes(router) {
    const routeGET = require('./routes/routeGET.js');
    const routePUT = require('./routes/routePUT.js');
    const routeDELETE = require('./routes/routeDELETE.js');
    const routeHEAD = require('./routes/routeHEAD.js');
    const datastore = {};
    const metastore = require('./testdata/metadata.json');

    /**
     * GET resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.get("/(.*)", function handleRequest(request, response) {
        routeGET(request, response, datastore, metastore);
    });

    /**
     * PUT resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.put('/:resource', function handleRequest(request, response) {
        routePUT(request, response, datastore, metastore);
    });

    /**
     * PUTRAW object - called when data is not received in the body but
     * as part of request stream in chunks
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.putraw('/:resource/(.*)', function handleRequest(request, response) {
        routePUT(request, response, datastore, metastore);
    });

    /**
     * DELETE resource - deletes bucket or object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */

    router.delete('/(.*)', function handleRequest(request, response) {
        routeDELETE(request, response, datastore, metastore);
    });

    /**
     * HEAD resource - retrieves metadata for bucket or object without content
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */
    router.head("/(.*)", function handleRequest(request, response) {
        routeHEAD(request, response, datastore, metastore);
    });
};
