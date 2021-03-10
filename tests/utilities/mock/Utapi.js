const express = require('express');

class Utapi {
    constructor() {
        this._server = null;
        this._port = 8100;
        this._data = {
            account: {
                incomingBytes: 0,
                outgoingBytes: 0,
                numberOfObjects: 0,
            },
            events: [],
        };
        this._app = express();
    }

    _parseData(req, res, next) {
        const requestBody = [];
        req.on('data', chunks => {
            requestBody.push(chunks);
        });
        req.on('end', () => {
            const parsedData = Buffer.concat(requestBody).toString();
            const events = JSON.parse(parsedData);
            // eslint-disable-next-line no-param-reassign
            req.body = events;
            next();
        });
    }

    _initiateRoutes() {
        this._app.post('/v2/ingest', this._parseData, (req, res) => {
            const event = req.body;
            this._updateData(event);
            res.status(200).end();
        });
    }

    _calculateMetric(metric) {
        return this._data.events.reduce((acc, curr) => {
            const delta = Number.parseInt(curr[metric], 10) || 0;
            return acc + delta;
        }, 0);
    }

    _updateData(events) {
        this._data.events.push(...events);
        const incomingBytes = this._calculateMetric('sizeDelta');
        const outgoingBytes = this._calculateMetric('outgoingBytes');
        const numberOfObjects = this._calculateMetric('objectDelta');
        this._data.account = {
            incomingBytes,
            outgoingBytes,
            numberOfObjects,
        };
    }

    getAccountMetrics() {
        return this._data.account;
    }

    reset() {
        this._data.events = [];
        this._data.account = {
            incomingBytes: 0,
            outgoingBytes: 0,
            numberOfObjects: 0,
        };
    }

    start() {
        this._initiateRoutes();
        this._server = this._app.listen(this._port);
    }

    stop() {
        this._server.close();
    }
}

module.exports = Utapi;
