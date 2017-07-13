'use strict'; // eslint-disable-line strict
const http = require('http');

const conf = require('../config.json');

const transport = http;

function options(token = 'report-token-1') {
    return {
        host: conf.ipAddress,
        path: '/_/report',
        port: 8000,
        headers: { 'x-scal-report-token': token },
    };
}

function queryReport(done, resultCheck) {
    const req = transport.request(options(), res => {
        res.on('error', done);

        if (res.statusCode !== 200) {
            return done(new Error(`non-200 status ${res.statusCode}`));
        }

        // the whole response should fit in 1 data event,
        // no need to incrementally build a buffer
        res.on('data', buf => resultCheck(JSON.parse(buf)));
        return undefined;
    });
    req.on('error', done).end();
}

describe('Report route', () => {
    it('should return 403 if given bad token', done => {
        const req = transport.request(options('bad-token'), res => {
            if (res.statusCode !== 403) {
                return done(new Error(`non-200 status ${res.statusCode}`));
            }
            return done();
        });
        req.on('error', done).end();
    });

    it('should return 200', done => {
        queryReport(done, () => done());
    });

    it('should contain a deployment uuid', done => {
        queryReport(done, response => {
            if (!response.uuid) {
                return done(new Error('response missing UUID'));
            }
            return done();
        });
    });

    it('should contain config', done => {
        queryReport(done, response => {
            if (!response.config || !response.config.locationConstraints) {
                return done(new Error('response missing config'));
            }
            return done();
        });
    });

    it('should remove authentication data from config', done => {
        queryReport(done, response => {
            if (response.config && response.config.authData) {
                return done(new Error('response config contains auth data'));
            }
            return done();
        });
    });

    it('should remove report token from config', done => {
        queryReport(done, response => {
            if (response.config && response.config.reportToken) {
                return done(new Error('response config contains report token'));
            }
            return done();
        });
    });
});
