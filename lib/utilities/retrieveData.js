const { eachOf, eachSeries, each } = require('async');
const responseErr = new Error();
responseErr.code = 'ResponseError';
responseErr.message = 'response closed by client request before all data sent';

function destroyStreams(res, readables) {
    each(readables, (r, n) => {
        r.destroy();
        return n();
    });
    res.destroy();
}

export default function retrieveData(locations, retrieveDataFn, response, log) {
    const readables = [];
    let streamError = false;

    response.once('close', () => {
        log.debug('received close event before response end');
        streamError = true;
    });

    response.once('error', err => {
        log.debug('received error event before response end', {
            error: err,
        });
        streamError = true;
    });

    eachOf(locations,
        (loc, key, next) => retrieveDataFn(loc, log, (err, readable) => {
            if (err) {
                return next(err);
            }
            readable.once('error', () => {
                streamError = true;
            });
            readables[key] = readable;
            return next();
        }), err => {
            if (err || streamError) {
                log.error('error from one location, aborting', {
                    error: err || 'error on readable stream',
                });
                destroyStreams(response, readables);
                return response.end();
            }
            return eachSeries(readables, (readable, next) => {
                readable.once('error', next);
                readable.once('end', next);
                readable.pipe(response, { end: false });
            }, err => {
                if (err || streamError) {
                    log.error('error from stream of one location, aborting', {
                        error: err || 'error on readable stream',
                    });
                    destroyStreams(response, readables);
                }
                return response.end();
            });
        }
    );
}
