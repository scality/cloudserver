import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';

export default (router) => {
    /**
     * GET resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.get("/(.*)", routeGET);

    /**
     * PUT resource - supports both bucket and object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.put('/(.*)', routePUT);

    /**
     * PUTRAW object - called when data is not received in the body but
     * as part of request stream in chunks
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.putraw('/:resource/(.*)', routePUT);


    /**
     * POST to initiate and complete multipart upload
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback - Callback to be called upon completion
     */
    router.post('/(.*)', routePOST);

    /**
     * DELETE resource - deletes bucket or object
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */

    router.delete('/(.*)', routeDELETE);

    /**
     * HEAD resource - retrieves metadata for bucket or object without content
     * @param {string} path style - It can be /<bucket name> or /<object name>
     * @param {function} callback with request and response objects
     */
    router.head("/(.*)", routeHEAD);
};
