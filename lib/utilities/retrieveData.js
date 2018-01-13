const { eachSeries } = require('async');
const responseErr = new Error();
responseErr.code = 'ResponseError';
responseErr.message = 'response closed by client request before all data sent';

export default function retrieveData(locations, retrieveDataFn, response, log) {
    let responseDestroyed = false;
    const _destroyResponse = () => {
        // destroys the socket if available
        response.destroy();
        responseDestroyed = true;
    };
    response.once('close', () => {
        log.debug('received close event before response end');
        _destroyResponse();
    });

    eachSeries(locations,
        (current, next) => retrieveDataFn(current, log, (err, readable) => {
            let cbCalled = false;
            const _next = err => {
                // Avoid multiple callbacks since it's possible that response's
                // close event and the readable's end event are emitted at
                //  the same time.
                if (!cbCalled) {
                    cbCalled = true;
                    next(err);
                }
            };

            if (err) {
                log.error('failed to get object', {
                    error: err,
                    method: 'retrieveData',
                });
                _destroyResponse();
                return _next(err);
            }
            if (responseDestroyed) {
                log.debug('response destroyed before readable could stream');
                readable.emit('close');
                return _next(responseErr);
            }
            // client closed the connection abruptly
            response.once('close', () => {
                log.debug('received close event before readable end');
                if (!responseDestroyed) {
                    _destroyResponse();
                }
                readable.emit('close');
                return _next(responseErr);
            });
            // readable stream successfully consumed
            readable.on('end', () => {
                log.debug('readable stream end reached');
                return _next();
            });
            // errors on server side with readable stream
            readable.on('error', err => {
                log.error('error piping data from source');
                return _next(err);
            });
            readable.on('data', chunk => {
                const write = response.write(chunk);
                if (!write) {
                    readable.pause();
                    response.once('drain', () => {
                        readable.resume();
                    });
                }
            });
            return undefined;
        }), err => {
            if (err) {
                log.debug('abort response due to client error', {
                    error: err.code, errMsg: err.message });
            }
            // call end for all cases (error/success) per node.js docs
            // recommendation
            response.end();
        }
    );
}
