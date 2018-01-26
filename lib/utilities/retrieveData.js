const onFinished = require('on-finished');
const destroy = require('destroy');
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
    if (onFinished.isFinished(response)) {
        log.debug('response finished before retrieved data');
        return _destroyResponse();
    }

    return eachSeries(locations,
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
            if (onFinished.isFinished(response)) {
                log.debug('response destroyed before readable could stream');
                readable.emit('close');
                destroy(readable);
                return _next(responseErr);
            }
            // client closed the connection abruptly
            // response.once('close', () => {
            //     log.debug('received close event before readable end');
            //     if (!responseDestroyed) {
            //         _destroyResponse();
            //     }
            //     readable.emit('close');
            //     // readable.unpipe(response);
            //     readable = null;
            //     return _next(responseErr);
            // });
            onFinished(response, err => {
                if (err) {
                    log.error('response finished with error',
                    { error: err.code });
                    readable.emit('close');
                    readable.unpipe();
                    destroy(readable);
                }
                return _next(responseErr);
            });
            readable.on('end', () => {
                log.debug('readable stream end reached');
                return _next();
            });
            // errors on server side with readable stream
            readable.on('error', err => {
                log.error('error piping data from source');
                return _next(err);
            });
            return readable.pipe(response, { end: false });
        }), err => {
            if (err) {
                log.debug('abort response due to client error', {
                    error: err.code, errMsg: err.message });
            }
            // call end for all cases (error/success) per node.js docs
            // recommendation
            if (!onFinished.isFinished(response)) {
                response.removeAllListeners();
                response.end();
            }
        }
    );
}
