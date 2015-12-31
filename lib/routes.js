import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';

export default (router, logger) => {
    /**
     * GET resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.get("/(.*)", (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing GET: ${req.url}`);
        routeGET(req, res, log);
    });

    /**
     * PUT resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.put('/(.*)', (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing PUT: ${req.url}`);
        routePUT(req, res, log);
    });

    /**
     * PUTRAW object - called when data is not received in the body but
     * as part of request stream in chunks
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.putraw('/:resource/(.*)', (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing PUTRAW: ${req.url}`);
        routePUT(req, res, log);
    });


    /**
     * POST to initiate and complete multipart upload
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.post('/(.*)', (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing POST: ${req.url}`);
        routePOST(req, res, log);
    });

    /**
     * DELETE resource - deletes bucket or object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */

    router.delete('/(.*)', (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing DELETE: ${req.url}`);
        routeDELETE(req, res, log);
    });

    /**
     * HEAD resource - retrieves metadata for bucket or object without content
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */
    router.head("/(.*)", (req, res) => {
        const log = logger.newRequestLogger();
        log.debug(`Routing HEAD: ${req.url}`);
        routeHEAD(req, res, log);
    });
};
