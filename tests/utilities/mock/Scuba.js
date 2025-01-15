const { errors } = require('arsenal');
const express = require('express');
const { config } = require('../../../lib/Config');

const inflightFlushFrequencyMS = 200;

// eslint-disable-next-line no-extend-native
BigInt.prototype.toJSON = function toJSON() {
    return { $bigint: this.toString() };
};

class Scuba {
    constructor() {
        this._server = null;
        this._port = 8100;
        this._data = {
            bucket: new Map(),
        };
        this._app = express();
        this.supportsInflight = config.isQuotaInflightEnabled();
    }

    _initiateRoutes() {
        this._app.use(express.json());

        this._app.get('/health/deep', (req, res) => {
            const headerValue = req.header('error');
            if (headerValue) {
                return res.status(500).send(errors.InternalError);
            }
            return res.status(204).end();
        });

        this._app.post('/metrics/bucket/:bucket/latest', (req, res) => {
            let bucketName = req.params.bucket;
            if (!this.supportsInflight) {
                bucketName = req.params.bucket?.split('_')[0];
                return res.status(200).json({
                    bytesTotal: (this._data.bucket.get(bucketName)?.current || 0n).toString(),
                });
            }
            const inflight = BigInt(req.body?.inflight || 0);
            this._updateData({
                action: req.body?.action,
                bucket: bucketName,
                inflight,
            });
            const immediateInflights = req.body?.action === 'objectRestore' ? 0n : inflight;
            return res.json({
                bytesTotal: ((this._data.bucket.get(bucketName)?.current || 0n) +
                    (this._data.bucket.get(bucketName)?.nonCurrent || 0n) +
                    (this._data.bucket.get(bucketName)?.inflight || 0n) +
                    immediateInflights).toString(),
            });
        });
    }

    _updateData(event) {
        const { action, inflight, bucket } = event;
        let timeout = inflightFlushFrequencyMS;
        if (action === 'objectRestore') {
            timeout = 0;
        }
        if (!this._data.bucket.get(bucket)) {
            this._data.bucket.set(bucket, { current: 0n, nonCurrent: 0n, inflight: 0n });
        }
        if (timeout && this.supportsInflight) {
            setTimeout(() => {
                if (this._data.bucket.get(bucket)) {
                    // eslint-disable-next-line no-console
                    console.log(`Updating bucket ${bucket} with inflight ${inflight}`);
                    this._data.bucket.set(bucket, {
                        current: this._data.bucket.get(bucket).current,
                        nonCurrent: this._data.bucket.get(bucket).nonCurrent,
                        inflight: this._data.bucket.get(bucket).inflight + inflight,
                    });
                }
            }, timeout);
        } else {
            if (this._data.bucket.get(bucket)) {
                this._data.bucket.set(bucket, {
                    current: this._data.bucket.get(bucket).current,
                    nonCurrent: this._data.bucket.get(bucket).nonCurrent,
                    inflight: this._data.bucket.get(bucket).inflight + inflight,
                });
            }
        }
    }

    start() {
        this._initiateRoutes();
        this._server = this._app.listen(this._port);
    }

    reset() {
        this._data = {
            bucket: new Map(),
        };
    }

    stop() {
        this._server.close();
    }

    setInflightAsCapacity(bucketName) {
        if (!this.supportsInflight) {
            return;
        }
        this._data.bucket.forEach((value, key) => {
            if (key.startsWith(`${bucketName}_`)) {
                // eslint-disable-next-line no-param-reassign
                value.current += BigInt(value.inflight);
                // eslint-disable-next-line no-param-reassign
                value.inflight = 0n;
                this._data.bucket.set(key, value);
            }
        });
    }

    getInflightsForBucket(bucketName) {
        let inflightCount = 0n;
        this._data.bucket.forEach((value, key) => {
            if (!this.supportsInflight && key === bucketName) {
                inflightCount += (value.current + value.nonCurrent);
            } else if (this.supportsInflight && key.startsWith(`${bucketName}_`)) {
                inflightCount += value.inflight;
            }
        });
        return inflightCount;
    }

    incrementBytesForBucket(bucketName, bytes) {
        if (!this._data.bucket.has(bucketName)) {
            this._data.bucket.set(bucketName, { current: 0n, nonCurrent: 0n, inflight: 0n });
        }
        const bucket = this._data.bucket.get(bucketName);
        bucket.current += BigInt(bytes);
        this._data.bucket.set(bucketName, bucket);
    }
}

module.exports = {
    Scuba,
    inflightFlushFrequencyMS,
};
