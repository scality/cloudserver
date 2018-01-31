const { eachSeries } = require('async');
const responseErr = new Error();
responseErr.code = 'ResponseError';
responseErr.message = 'response closed by client request before all data sent';

export default function retrieveData(locations, retrieveDataFn, response, log) {
    // response is of type http.ServerResponse
    let responseDestroyed = false;
    let currentStream = null; // reference to the stream we are reading from
    const _destroyResponse = () => {
        // destroys the socket if available
        response.destroy();
        responseDestroyed = true;
    };
    // the S3-client might close the connection while we are processing it
    response.once('close', () => {
        log.debug('received close event before response end');
        responseDestroyed = true;
        if (currentStream) {
            currentStream.destroy();
        }
    });

    eachSeries(locations,
        (current, next) => retrieveDataFn(current, log, (err, readable) => {
            // NB: readable is of IncomingMessage type
            if (err) {
                log.error('failed to get object', {
                    error: err,
                    method: 'retrieveData',
                });
                _destroyResponse();
                return next(err);
            }
            // response.isclosed is set by the S3 server. Might happen if the
            // S3-client closes the connection before the first request to
            // the backend is started.
            if (responseDestroyed || response.isclosed) {
                log.debug('response destroyed before readable could stream');
                readable.destroy();
                return next(responseErr);
            }
            // readable stream successfully consumed
            readable.on('end', () => {
                currentStream = null;
                log.debug('readable stream end reached');
                return next();
            });
            // errors on server side with readable stream
            readable.on('error', err => {
                log.error('error piping data from source');
                _destroyResponse();
                return next(err);
            });
            currentStream = readable;
            return readable.pipe(response, { end: false });
        }), err => {
            currentStream = null;
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
