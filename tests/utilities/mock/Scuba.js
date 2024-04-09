const { errors } = require('arsenal');
const express = require('express');

const inflightFlushFrequencyMS = 200;

class Scuba {
    constructor() {
        this._server = null;
        this._port = 8100;
        this._data = {
            bucket: new Map(),
        };
        this._app = express();
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
            const bucketName = req.params.bucket;
            const inflight = Number(req.body?.inflight) || 0;
            this._updateData({
                action: req.body?.action,
                bucket: bucketName,
                inflight,
            });
            const immediateInflights = req.body?.action === 'objectRestore' ? 0 : inflight;
            res.json({
                bytesTotal: (this._data.bucket.get(bucketName)?.current || 0) +
                    (this._data.bucket.get(bucketName)?.nonCurrent || 0) +
                    (this._data.bucket.get(bucketName)?.inflight || 0) +
                    immediateInflights,
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
            this._data.bucket.set(bucket, { current: 0, nonCurrent: 0, inflight: 0 });
        }
        if (timeout) {
            setTimeout(() => {
                if (this._data.bucket.get(bucket)) {
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

    getInflightsForBucket(bucketName) {
        let inflightCount = 0;
        this._data.bucket.forEach((value, key) => {
            if (key.startsWith(`${bucketName}_`)) {
            inflightCount += value.inflight;
            }
        });
        return inflightCount;
    }
}

module.exports = {
    Scuba,
    inflightFlushFrequencyMS,
};
