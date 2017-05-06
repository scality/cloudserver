class Middleware {
    constructor() {
        this.order = [
            'normalizeRequest',
            'capturePostData',
            'router',
            'auth',
        ];
    }

    exec(req, res, cb) {

    }
}

module.exports = Middleware;
