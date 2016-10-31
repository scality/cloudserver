import path from 'path';
import domain from 'domain';
import stream from 'stream';
import { errors } from 'arsenal';
import validate from './validate';

const PredicateError = require('swerrs').extend({
    name: 'PredicateError',
});

const eventInfoSpec = {
    // TODO: expand to conver desired event types
    eventName: /^ObjectCreated:Put$/,
    // TODO: what is the real bucket naming spec?
    bucket: /^[\w\-\.]+$/,
    // TODO: make a regexp out of the prefix and cache it
    prefix(v) {
        if (!v) {
            this.prefix = '';
            return true;
        }
        return (typeof v === 'string');
    },
};

const metadataRe = /^x\-amz\-meta\-([\w\-]+)$/;

const runParamsSpec = {
    eventName: eventInfoSpec.eventName,
    bucket: eventInfoSpec.bucket,
    key: /\w+/,
    // TODO: improve field extraction
    request(req) {
        if (!(req instanceof stream.Readable)) {
            return false;
        }
        const errs = validate({
            headers: {},
            namespace: '',
            parsedHost(ip) {
                if (!ip || typeof ip !== 'string') {
                    return false;
                }
                const parts = ip.split('.');
                if (parts.length !== 4) {
                    return false;
                }
                return parts.every(p => {
                    if (isNaN(p) || p > 255) {
                        return false;
                    }
                    return true;
                });
            },
            parsedContentLength: 0,
            contentMD5: /^[0-9a-f]{32}$/i,
        }, req);
        if (errs.length) {
            return false;
        }
        this.headers = req.headers;
        this.region = req.namespace;
        this.sourceIPAddress = req.parsedHost;
        this.size = req.parsedContentLength;
        this.etag = req.contentMD5;
        this.contentType = req.headers['content-type'];
        this.metadata = {};
        Object.keys(req.headers).forEach(k => {
            const m = metadataRe.exec(k);
            if (!m) {
                return;
            }
            this.metadata[m[1]] = req.headers[k];
        });
        return true;
    },
};

function createKey(eventInfo) {
    return `${eventInfo.eventName}|${eventInfo.bucket}`;
}

// TODO: improve field extraction
function makePredicateEvent(runParams, buf) {
    return {
        Records: [{
            eventVersion: '2.0',
            eventSource: 'aws:s3',
            awsRegion: runParams.region,
            eventTime: new Date(),
            eventName: runParams.eventName,
            userIdentity: {
                principalId: runParams.userId,
            },
            requestParameters: {
                sourceIPAddress: runParams.sourceIPAddress,
            },
            responseElements: {
                'x-amz-request-id': runParams.amzRequestId,
                'x-amz-id-2': runParams.amzRequestId2,
            },
            s3: {
                s3SchemaVersion: '1.0',
                configurationId: '00000000-0000-0000-0000-000000000000',
                bucket: {
                    name: runParams.bucket,
                    ownerIdentity: {
                        principalId: runParams.bucketOwnerId,
                    },
                    arn: runParams.bucketArn,
                },
                object: {
                    key: runParams.key,
                    size: runParams.size,
                    eTag: runParams.etag,
                    contentType: runParams.contentType,
                    metadata: runParams.metadata,
                    data: buf,
                    versionId: 'N/A',
                    sequencer: 'N/A',
                },
            },
        }],
    };
}

function tryRequire(p) {
    const out = {};
    try {
        const fn = require(path.resolve(__dirname, p));
        if (typeof fn !== 'function') {
            throw new TypeError('Required object not function');
        }
        out.fn = fn;
    } catch (e) {
        out.err = e;
    }
    return out;
}

function getPredicate(registry, runParams, callback) {
    const ve =
        validate(runParamsSpec, runParams, new PredicateError(), runParams);
    if (ve.hasValues()) {
        return callback(ve);
    }

    const key = createKey(runParams);
    const preds = registry.predicates[key];

    if (!preds) {
        return callback(ve.push(`No predicate exists for "${key}"`));
    }

    const k = runParams.key;
    const klen = k.length;
    let maxp = -1;
    let fn = null;

    // Linear search - for now we don't anticipate
    // having too many prefixes for the same bucket.
    Object.keys(preds).forEach(p => {
        const plen = p.length;
        if (plen > klen) {
            return;
        }
        if (plen > maxp && k.indexOf(p) === 0) {
            maxp = plen;
            fn = preds[p];
        }
    });

    if (!fn) {
        return callback(ve.push(`No predicate exists for "${key}"`));
    }
    return callback(null, fn);
}

function runWrapped(context, callback) {
    const { passthrough, event, fn } = context;
    // NOTE: Domains are used to prevent exceptions from
    // killing the worker process (which is not possible with
    // promises). See tests for details.
    const d = domain.create();
    d.on('error', err => {
        d.exit();
        return callback(err);
    });
    d.run(() => fn(event, (err, res, body) => {
        d.exit();
        if (err) {
            return callback(new PredicateError(err));
        }
        if (!body) {
            return callback(null, res, passthrough);
        }
        // The user-supplied predicated transformed the object.
        const output = new stream.PassThrough();
        if (body instanceof stream.Readable) {
            const chunks = [];
            body.on('data', c => {
                chunks.push(c);
                output.write(c);
            });
            body.on('end', () => {
                output.parsedContentLength =
                            Buffer.concat(chunks).length;
                output.end();
            });
        } else {
            output.parsedContentLength = Buffer.byteLength(body);
            output.end(body);
        }
        return callback(null, res, output);
    }));
}

function runPredicate(runParams, fn, callback) {
    const { request } = runParams;
    const chunks = [];
    const passthrough = new stream.PassThrough();
    passthrough.headers = request.headers; // TODO: do we need anything else??
    passthrough.parsedContentLength = request.parsedContentLength;

    request.on('data', c => {
        chunks.push(c);
        passthrough.write(c);
    });
    request.on('error', err => {
        passthrough.end();
        return callback(err);
    });
    request.on('end', () => {
        passthrough.end();
        return runWrapped({
            passthrough,
            event: makePredicateEvent(runParams, Buffer.concat(chunks)),
            fn,
        }, (err, res, output) => {
            if (err) {
                return callback(err);
            }
            // eslint-disable-next-line no-param-reassign
            output.headers = request.headers;
            return callback(null, res, output);
        });
    });
}

class PredicateRegistry {

    constructor() {
        this.predicates = {};
    }

    /**
     * Add user-supplied predicate to the registry:
     *
     * @param {object} eventInfo - object conforming to eventInfoSpec,
     * @param {function | string} _fn - user-supplied predicate function,
     *                                  or path to same,
     * @param {function | undefined} _callback - called once registration
     *                                          is complete. If no callback
     *                                          is supplies, put throws.
     * @return {undefined}
     */
    put(eventInfo, _fn, _callback) {
        let callback;

        if (typeof _callback === 'function') {
            callback = _callback;
        } else {
            callback = err => {
                if (err) {
                    throw err;
                }
            };
        }

        const ve =
            validate(eventInfoSpec, eventInfo, new PredicateError(), eventInfo);
        if (ve.hasValues()) {
            return callback(ve);
        }

        let fn;

        if (typeof _fn === 'string') {
            const fnResult = tryRequire(_fn);
            if (fnResult.err) {
                return callback(fnResult.err);
            }
            fn = fnResult.fn;
        } else if (typeof _fn !== 'function') {
            return callback(ve.push(
                'User-supplied predicate must be either ' +
                'a function or a path to a function'));
        } else {
            fn = _fn;
        }

        if (fn.length !== 2) { // TODO: update
            return callback(ve.push(
                'User-supplied predicates must take 2 arguments'
            ));
        }

        const key = createKey(eventInfo);
        let preds = this.predicates[key];

        if (preds) {
            if (preds[eventInfo.prefix]) {
                return callback(ve.push(
                    `User-supplied predicate already exists for "${key}", ` +
                    `"${eventInfo.prefix}"`
                ));
            }
        } else {
            this.predicates[key] = preds = {};
        }

        preds[eventInfo.prefix] = fn;

        return callback();
    }

    /**
     * Run user-supplied predicate before matching object is stored:
     *
     * @param {object} runParams - object conforming to runParamsSpec,
     * @param {function} callback - called after predicate has processed
     *                              incoming object or in the case of error;
     *                              it is callback's responsibility to store
     *                              the data.
     * @return {undefined}
     */
    run(runParams, callback) {
        const { log } = runParams;
        getPredicate(this, runParams, (err, fn) => {
            if (err) {
                log.trace('predicate', 'no user-supplied predicate found');
                return callback(null, runParams.request);
            }
            return runPredicate(runParams, fn, (err, res, output) => {
                if (err) {
                    if (err instanceof PredicateError) {
                        return callback(errors.PreconditionFailed, output);
                    }
                    // user code threw or there was
                    // a problem reading the request.
                    log.trace(err);
                    return callback(errors.InternalError, output);
                }
                if (res) {
                    log.trace(res);
                }
                return callback(null, output);
            });
        });
    }

    purge() {
        this.predicates = {};
    }
}

export default new PredicateRegistry();
