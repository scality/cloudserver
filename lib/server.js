const CONFIG = {
    CLUSTERING: false,
    CLUSTERING_FORKS: 10
};
// <reference path="../typings/node/node.d.ts"/>
// const jsutils = require("jsutils");
// const logs = new jsutils.LogCtx('ironman', 'info', false, CONFIG.LOGDIR);
// const profiler = require('v8-profiler');
const cluster = require('cluster');

if (CONFIG.CLUSTERING && cluster.isMaster) {
    let n;
    for (n = 0; n < CONFIG.CLUSTERING_FORKS; n++) {
        cluster.fork();
    }

    cluster.on('disconnect', function onClusterDisconnect(worker) {
        console.error('worker' + worker.id + 'disconnected, start a new one');
        cluster.fork();
    });
} else {
    (function serverStartup() {
        let server;
        let cleanUp;

        const domain = require('domain');
        const http = require('http');
        const Router = require('node-simple-router');
        const router = new Router({
            'list_dir': true,
            'logging': true
        });
        require('./routes.js')(router);

        /*
         * Tune HTTP client behavior
         */
        // Todo: http.globalAgent.maxSockets, http.globalAgent.maxFreeSockets
        http.globalAgent.keepAlive = true;


        const argv = process.argv.slice(2);

        const httpServerResponder = function httpServerResponder(
            request,
            response
        ) {
            const d = domain.create();

            const recover =  function recover(er) {
                console.error("domain", 'error', er.stack);
                // Note: we're in dangerous territory!
                // By definition, something unexpected occurred,
                // which we probably didn't want.
                // Anything can happen now!  Be very careful!
                try {
                    // stop taking new requests.
                    if (CONFIG.CLUSTERING) {
                        // make sure we close down within 30 seconds
                        const killtimer = setTimeout(function timeoutHandler() {
                            process.exit(1);
                        }, 5000);
                        // But don't keep the process open just for that!
                        killtimer.unref();

                        server.close();
                        // Let the master know we're dead.  This will trigger a
                        // 'disconnect' in the cluster master, and then it will
                        // fork a new worker.
                        cluster.worker.disconnect();
                    }

                    // try to send an error to the request that triggered
                    //  the problem
                    response.statusCode = 500;
                    response.setHeader('content-type', 'text/plain');
                    response.end(
                        'Oops, there was a problem!\n' + er.toString()
                    );
                } catch (er2) {
                    // oh well, not much we can do at this point.
                    console.error('Error sending 500!', er2.stack);
                }
            };

            d.on('error', recover);

            d.run(function routerHandler() {
                router(request, response);
            });
        };

        server = http.createServer(httpServerResponder);
        server.setTimeout(CONFIG.HTTP_SERVER_TIMEOUT_MS);


        server.on('listening', function listener() {
            let addr;
            addr = server.address() || {
                address: '0.0.0.0',
                port: argv[0] || 8000
            };
            router.log(
                'Serving web content at ' +
                    addr.address +
                    ':' +
                    addr.port +
                    ' - PID: ' +
                    process.pid
            );
        });

        cleanUp = function cleanUp() {
            console.log(" ");
            console.log("Server shutting down...");
            console.log(" ");
            server.close();
            return process.exit(0);
        };

        process.on('SIGINT', cleanUp);
        process.on('SIGHUP', cleanUp);
        process.on('SIGQUIT', cleanUp);
        process.on('SIGTERM', cleanUp);
        process.on('SIGPIPE', function handler() {});
        let listenerPort = 8000;
        if ((argv[0] !== null) && !isNaN(parseInt(argv[0], 10))) {
            listenerPort = parseInt(argv[0], 10);
        }

        server.listen(listenerPort);
    }).call(this);
}
