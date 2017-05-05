const { normalizeRequest } = require('./utils');

class Router {

    constructor(routes, controllers) {
        this._routes = routes;
        this._controllers = controllers;
    }

    _matchRoute(route, req) {
        const query = route.query || [];
        return req.method === route.method &&
            Boolean(req.bucketName) === Boolean(route.bucket) &&
            Boolean(req.objectKey) === Boolean(route.object) &&
            query.every(q => q in req.query);
    }

    exec(request, response, cb) {
        const _req = normalizeRequest(request);
        const index = this._routes.findIndex(r => this._matchRoute(r, _req));

        if (index === -1) {
            // TODO: return NotImplemented / Invalid Request
        }

        const { controller, action } = this._routes[index];
        this._controllers[controller][action](request, response, cb);
    }
}

module.exports = Router;
