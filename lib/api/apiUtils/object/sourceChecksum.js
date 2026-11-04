const { PassThrough } = require('stream');
const async = require('async');
const { jsutil } = require('arsenal');

const { data } = require('../../../data/wrapper');

/**
 * Sequentially GET the ordered source `dataLocator` parts into a single
 * readable stream, reading them in order through `data.get`. Returns the
 * PassThrough immediately; consumers should pipe it onward and observe
 * `error` for any read failure along the way.
 *
 * @param {Array} dataLocator - ordered source parts
 * @param {object} log - request logger
 * @return {PassThrough}
 */
function buildSourcePartsStream(dataLocator, log) {
    const passthrough = new PassThrough();
    const wrapErr = (err, part) =>
        Object.assign(err, {
            copyPart: { key: part.key, dataStoreName: part.dataStoreName, dataStoreType: part.dataStoreType },
        });
    async.eachSeries(
        dataLocator,
        (part, cb) => {
            const done = jsutil.once(cb);
            if (part.dataStoreType === 'azure') {
                // Azure's data.get writes part bytes into the provided writable
                // instead of returning a Readable. Pipe a per-part PassThrough
                // into the master passthrough and use its 'end' as the completion
                // signal — same pattern arsenal's data.copyObject uses.
                const perPart = new PassThrough();
                perPart.once('error', err => done(wrapErr(err, part)));
                perPart.once('end', () => done());
                perPart.pipe(passthrough, { end: false });
                return data.get(part, perPart, log, err => {
                    if (err) {
                        perPart.destroy(err);
                        done(wrapErr(err, part));
                    }
                });
            }
            return data.get(part, null, log, (err, partStream) => {
                if (err) {
                    return done(wrapErr(err, part));
                }
                partStream.once('error', err => done(wrapErr(err, part)));
                partStream.once('end', () => done());
                partStream.pipe(passthrough, { end: false });
                return undefined;
            });
        },
        err => {
            if (err) {
                passthrough.destroy(err);
            } else {
                passthrough.end();
            }
        },
    );
    return passthrough;
}

module.exports = { buildSourcePartsStream };
