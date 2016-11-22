import path from 'path';
import net from 'net';
import stream from 'stream';
import { errors } from 'arsenal';
import utils from '../utils';
import validate from './validate';
import RequestWrapper from './requestWrapper';
import { OUTPUT as OUTPUT_SYM } from './requestWrapper';

const eventTypes = {
    'ObjectCreated:Put': 'ObjectCreated:Put',
    'objectPut': 'ObjectCreated:Put',
    // No AWS Lambda notifications currently exist for GETs,
    // so we're making this terminology up.
    // See http://docs.aws.amazon.com/
    // AmazonS3/latest/dev/NotificationHowTo.html#notification-how-to
    // -event-types-and-destinations
    'ObjectRetrieved:Get': 'ObjectRetrieved:Get',
    'objectGet': 'ObjectRetrieved:Get',
};

const metadataRe = /^x\-amz\-meta\-([\w\-]+)$/;

function cannonicalizeEventName(v) {
    if (!v || typeof v !== 'string') {
        return false;
    }
    const eventName = eventTypes[v];
    if (!eventName) {
        return false;
    }
    this.eventName = eventName;
    return true;
}

const eventInfoSpec = {
    // TODO: expand to conver desired event types
    eventName: cannonicalizeEventName,
    // TODO: make a regexp out of the prefix and cache it
    // in runParams when we validate through this function
    bucket: utils.isValidBucketName,
    prefix(v) {
        if (!v) {
            this.prefix = '';
            return true;
        }
        return (typeof v === 'string');
    },
};

const runParamsSpec = {
    eventName: cannonicalizeEventName,
    userInfo(v) {
        return v && typeof v.getShortid === 'function';
    },
    request(req) {
        if (!(req instanceof stream.Readable)) {
            return false;
        }
        const errs = validate({
            bucketName: utils.isValidBucketName,
            objectKey: /\w+/,
            headers: {},
            namespace: '',
            parsedHost(ip) {
                return !!net.isIP(ip);
            },
            parsedContentLength: 0,
            contentMD5: /^[0-9a-f]{32}$/i,
        }, req);
        if (errs.length) {
            return false;
        }
        this.bucket = req.bucketName;
        this.key = req.objectKey;
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
    log(v) {
        if (typeof v !== 'object') {
            return false;
        }
        return ['trace', 'info'].every(k => typeof v[k] === 'function');
    },
};

function createKey(eventInfo) {
    return `${eventInfo.eventName}|${eventInfo.bucket}`;
}

const s3ObjectMapping = {
    key(v) {
        if (this.objectKey !== v) {
            this.objectKey = v;
        }
    },
    contentType(v) {
        if (this.headers['content-type'] !== v) {
            this.headers['content-type'] = v;
        }
    },
    metadata(v) {
        Object.keys(v).forEach(k => {
            const tag = k.toLowerCase();
            this.headers[`x-amz-meta-${tag}`] = v[k];
        });
    },
};

function updateEventInfo(event, output) {
    const s3Obj = event.Records[0].s3.object;
    Object.keys(s3ObjectMapping).forEach(k => {
        s3ObjectMapping[k].call(output, s3Obj[k]);
    });
    return output;
}

// TODO: improve field extraction
function makePredicateEvent(runParams, _body) {
    const body = runParams.eventName === 'ObjectCreated:Put' ?
        _body : null;
    return {
        Records: [{
            eventVersion: '2.0',
            eventSource: 'aws:s3',
            awsRegion: runParams.region,
            eventTime: new Date(),
            eventName: runParams.eventName,
            userIdentity: {
                principalId: runParams.userInfo.getShortid(),
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
                    body,
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
        const fnpath = path.resolve(process.cwd(), p);
        const fn = require(fnpath);
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
    const errs =
        validate(runParamsSpec, runParams, runParams);
    if (errs.length) {
        return callback('Invalid runParams');
    }

    const key = createKey(runParams);
    const preds = registry.predicates[key];

    if (!preds) {
        return callback(`No predicate exists for "${key}"`);
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
        return callback(`No predicate exists for "${key}"`);
    }
    return callback(null, fn);
}

function runWrapped(event, fn, log, callback) {
    // NOTE: unexpected errors in the predicate will kill
    // the worker process, but short of diving into `vm` or
    // `async_wrap`, I don't see how to handle buggy async user
    // predicate functions without resorting to `domain`.
    // `vm` would be a more robust approach to sandboxing user code,
    // but that's another PR.

    // TODO: the second, "context", argument is here to mimic the AWS
    // S3 Lambda API. It doesn't currently do anything, but it could.

    fn(event, {}, err => {
        if (err) {
            const predError = (err instanceof Error) ? err : new Error(err);
            log.trace('operation rejected by user predicate', predError);
            return callback(errors.PreconditionFailed);
        }
        return callback();
    });
}

function runPredicate(runParams, fn, callback) {
    const { request, log } = runParams;
    const reqWrap = new RequestWrapper(request);
    const event = makePredicateEvent(runParams, reqWrap);

    return runWrapped(event, fn, log, err => {
        if (err) {
            return callback(err);
        }
        const output = updateEventInfo(event, reqWrap[OUTPUT_SYM]());
        return callback(null, output);
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
     *                                  or path to same
     * @return {undefined}
     */
    put(eventInfo, _fn) {
        const errs =
            validate(eventInfoSpec, eventInfo, eventInfo);
        if (errs.length) {
            throw new Error(errs.join());
        }

        let fn;

        if (typeof _fn === 'string') {
            const fnResult = tryRequire(_fn);
            if (fnResult.err) {
                throw fnResult.err;
            }
            fn = fnResult.fn;
        } else if (typeof _fn !== 'function') {
            throw new Error('User-supplied predicate must be either ' +
                'a function or a path to a function');
        } else {
            fn = _fn;
        }

        // Mimic AWS Lambda even though we currently don't use "context"
        if (fn.length !== 3) {
            throw new Error('User-supplied predicates must take 3 arguments');
        }

        const key = createKey(eventInfo);
        let preds = this.predicates[key];

        if (preds) {
            if (preds[eventInfo.prefix]) {
                throw new Error('User-supplied predicate already exists ' +
                    `for "${key}", "${eventInfo.prefix}"`);
            }
        } else {
            this.predicates[key] = preds = {};
        }

        preds[eventInfo.prefix] = fn;
    }

    hasPredicates() {
        return Object.keys(this.predicates).length > 0;
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
        if (!this.hasPredicates()) {
            return callback(null, runParams.request);
        }
        const { log } = runParams;
        return getPredicate(this, runParams, (err, fn) => {
            if (err) {
                log.trace('predicate', {
                    message: 'no user-supplied predicate found',
                });
                return callback(null, runParams.request);
            }
            return runPredicate(runParams, fn, (err, output) => {
                callback(err, output);
            });
        });
    }

    // Used for testing only
    purge() {
        this.predicates = {};
    }
}

export default new PredicateRegistry();
