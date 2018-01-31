const onFinished = require('on-finished');
const destroy = require('destroy');
const { eachSeries } = require('async');
const responseErr = new Error();
responseErr.code = 'ResponseError';
responseErr.message = 'response closed by client request before all data sent';


export default function retrieveData(locations, retrieveDataFn, response, log) {
    let currentReadable;
    const _destroyResponse = () => {
        // destroys the socket if available
        response.destroy();
    };

    onFinished(response, err => {
        if (err) {
            log.error('response finished with error',
            { error: err.code });
            if (currentReadable) {
                currentReadable.emit('abortSourceNow');
                destroy(currentReadable);
                process.nextTick(() => {
                    currentReadable = null;
                });
            }
        }

        if (onFinished.isFinished(response)) {
            log.debug('response finished before retrieved data');
            currentReadable = null;
            return _destroyResponse();
        }
    });

    return eachSeries(locations,
        (current, next) => retrieveDataFn(current, log, (err, readable) => {
            let cbCalled = false;
            currentReadable = readable;
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
                // just in case we hit this before the event
                // listener in the event loop, avoid starting
                // the pipe
                log.debug('response destroyed before readable could stream');
                readable.emit('abortSourceNow');
                destroy(readable);
                return _next(responseErr);
            }
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
            currentReadable = null;
            if (err) {
                log.debug('abort response due to client error', {
                    error: err.code, errMsg: err.message });
            }
            // call end for all cases (error/success) per node.js docs
            // recommendation
            if (!onFinished.isFinished(response)) {
                response.end();
            }
        }
    );
}
