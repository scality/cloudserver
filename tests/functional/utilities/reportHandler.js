const assert = require('assert');
const http = require('http');

const { ConfigObject } = require('../../../lib/Config');
const config = new ConfigObject();

const { DummyRequestLogger } = require('../../unit/helpers');

const logger = new DummyRequestLogger();

const testLocationConstraints = {
    site1: { type: 'aws_s3' },
    site2: { type: 'aws_s3' },
};
config.setReplicationEndpoints(testLocationConstraints);
config.backbeat = { host: 'localhost', port: 4242 };

const {
    _crrMetricRequest,
    getCRRMetrics,
    getReplicationStates,
    _ingestionMetricRequest,
    getIngestionMetrics,
    getIngestionStates,
    getIngestionInfo,
} = require('../../../lib/utilities/reportHandler');

const crrExpectedResultsRef = {
    completions: { count: 10000, size: 10000 },
    failures: { count: 2000, size: 2000 },
    pending: { count: 8000, size: 8000 },
    backlog: { count: 10000, size: 10000 },
    throughput: { count: 11, size: 11 },
    byLocation: {
        site1: {
            completions: { count: 5000, size: 5000 },
            failures: { count: 1000, size: 1000 },
            pending: { count: 4000, size: 4000 },
            backlog: { count: 5000, size: 5000 },
            throughput: { count: 5, size: 5 },
        },
        site2: {
            completions: { count: 5000, size: 5000 },
            failures: { count: 1000, size: 1000 },
            pending: { count: 4000, size: 4000 },
            backlog: { count: 5000, size: 5000 },
            throughput: { count: 5, size: 5 },
        },
    },
};
const ingestionExpectedResultsRef = {
    completions: { count: 4000 },
    pending: { count: 10000 },
    throughput: { count: 15 },
    byLocation: {
        site1: {
            completions: { count: 2000 },
            pending: { count: 5000 },
            throughput: { count: 7 },
        },
        site2: {
            completions: { count: 2000 },
            pending: { count: 5000 },
            throughput: { count: 7 },
        },
    },
};

const crrRequestResults = {
    all: {
        completions: { results: { count: 10000, size: 10000 } },
        failures: { results: { count: 2000, size: 2000 } },
        pending: { results: { count: 8000, size: 8000 } },
        backlog: { results: { count: 10000, size: 10000 } },
        throughput: { results: { count: 11, size: 11 } },
    },
    site1: {
        completions: { results: { count: 5000, size: 5000 } },
        failures: { results: { count: 1000, size: 1000 } },
        pending: { results: { count: 4000, size: 4000 } },
        backlog: { results: { count: 5000, size: 5000 } },
        throughput: { results: { count: 5, size: 5 } },
    },
    site2: {
        completions: { results: { count: 5000, size: 5000 } },
        failures: { results: { count: 1000, size: 1000 } },
        pending: { results: { count: 4000, size: 4000 } },
        backlog: { results: { count: 5000, size: 5000 } },
        throughput: { results: { count: 5, size: 5 } },
    },
};
const ingestionRequestResults = {
    all: {
        completions: { results: { count: 4000 } },
        pending: { results: { count: 10000 } },
        throughput: { results: { count: 15 } },
    },
    site1: {
        completions: { results: { count: 2000 } },
        pending: { results: { count: 5000 } },
        throughput: { results: { count: 7 } },
    },
    site2: {
        completions: { results: { count: 2000 } },
        pending: { results: { count: 5000 } },
        throughput: { results: { count: 7 } },
    },
};

const expectedStatusResults = {
    site1: 'enabled',
    site2: 'disabled',
};

const expectedScheduleResults = {
    site1: 'none',
    site2: new Date(),
};

function requestFailHandler(req, res) {
    const testError = {
        code: 404,
        description: 'reportHandler test error',
    };
    // eslint-disable-next-line no-param-reassign
    res.statusCode = 404;
    res.write(JSON.stringify(testError));
    res.end();
}

function requestHandler(req, res) {
    const { url } = req;
    if (url.startsWith('/_/metrics/crr/')) {
        const site = url.split('/_/metrics/crr/')[1] || '';
        if (crrRequestResults[site]) {
            res.write(JSON.stringify(crrRequestResults[site]));
        }
    } else if (url.startsWith('/_/metrics/ingestion/')) {
        const site = url.split('/_/metrics/ingestion/')[1] || '';
        if (ingestionRequestResults[site]) {
            res.write(JSON.stringify(ingestionRequestResults[site]));
        }
    } else {
        switch (req.url) {
        case '/_/crr/status':
        case '/_/ingestion/status':
            res.write(JSON.stringify(expectedStatusResults));
            break;
        case '/_/crr/resume/all':
        case '/_/ingestion/resume/all':
            res.write(JSON.stringify(expectedScheduleResults));
            break;
        default:
            break;
        }
    }
    res.end();
}

[
    { method: _crrMetricRequest, result: crrExpectedResultsRef },
    { method: _ingestionMetricRequest, result: ingestionExpectedResultsRef },
].forEach(item => {
    describe(`reportHandler::${item.method.name}`, function testSuite() {
        this.timeout(20000);
        const testPort = '4242';
        let httpServer;

        describe('Test Request Failure Cases', () => {
            before(done => {
                httpServer = http.createServer(requestFailHandler)
                                 .listen(testPort);
                httpServer.on('listening', done);
                httpServer.on('error', err => {
                    process.stdout.write(`https server: ${err.stack}\n`);
                    process.exit(1);
                });
            });

            after('Terminating Server', () => {
                httpServer.close();
            });

            it('should return empty object if a request error occurs',
            done => {
                const endpoint = 'http://nonexists:4242';
                item.method(endpoint, 'all', logger, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {});
                    done();
                });
            });

            it('should return empty object if response status code is >= 400',
            done => {
                const endpoint = 'http://localhost:4242';
                item.method(endpoint, 'all', logger, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {});
                    done();
                });
            });
        });

        describe('Test Request Success Cases', () => {
            const endpoint = 'http://localhost:4242';
            before(done => {
                httpServer = http.createServer(requestHandler)
                                 .listen(testPort);
                httpServer.on('listening', done);
                httpServer.on('error', err => {
                    process.stdout.write(`https server: ${err.stack}\n`);
                    process.exit(1);
                });
            });

            after('Terminating Server', () => {
                httpServer.close();
            });

            it('should return correct location metrics', done => {
                item.method(endpoint, 'site1', logger, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(
                        res, item.result.byLocation.site1);
                    done();
                });
            });
        });
    });
});

[
    { method: getCRRMetrics, result: crrExpectedResultsRef },
    { method: getIngestionMetrics, result: ingestionExpectedResultsRef },
].forEach(item => {
    describe(`reportHandler::${item.method.name}`, function testSuite() {
        this.timeout(20000);
        const testPort = '4242';
        let httpServer;

        describe('Test Request Success Cases', () => {
            before(done => {
                httpServer = http.createServer(requestHandler).listen(testPort);
                httpServer.on('listening', done);
                httpServer.on('error', err => {
                    process.stdout.write(`https server: ${err.stack}\n`);
                    process.exit(1);
                });
            });

            after('Terminating Server', () => {
                httpServer.close();
            });

            it('should return correct results', done => {
                if (item.method.name === 'getIngestionMetrics') {
                    const sites = ['site1', 'site2'];
                    item.method(sites, logger, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, item.result);
                        done();
                    }, config);
                } else {
                    item.method(logger, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, item.result);
                        done();
                    }, config);
                }
            });
        });
    });
});

[
    { method: getReplicationStates },
    { method: getIngestionStates },
].forEach(item => {
    describe(`reportHandler::${item.method.name}`, function testSuite() {
        this.timeout(20000);
        const testPort = '4242';
        let httpServer;

        describe('Test Request Failure Cases', () => {
            before(done => {
                httpServer = http.createServer(requestFailHandler)
                                 .listen(testPort);
                httpServer.on('listening', done);
                httpServer.on('error', err => {
                    process.stdout.write(`https server: ${err.stack}\n`);
                    process.exit(1);
                });
            });

            after('Terminating Server', () => {
                httpServer.close();
            });

            it('should return empty object if a request error occurs',
            done => {
                item.method(logger, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {});
                    done();
                }, { backbeat: { host: 'nonexisthost', port: testPort } });
            });

            it('should return empty object if response status code is >= 400',
            done => {
                item.method(logger, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {});
                    done();
                }, { backbeat: { host: 'localhost', port: testPort } });
            });
        });

        describe('Test Request Success Cases', () => {
            before(done => {
                httpServer = http.createServer(requestHandler)
                                 .listen(testPort);
                httpServer.on('listening', done);
                httpServer.on('error', err => {
                    process.stdout.write(`https server: ${err.stack}\n`);
                    process.exit(1);
                });
            });

            after('Terminating Server', () => {
                httpServer.close();
            });

            it('should return correct results', done => {
                item.method(logger, (err, res) => {
                    const expectedResults = {
                        states: {
                            site1: 'enabled',
                            site2: 'disabled',
                        },
                        schedules: {
                            site2: expectedScheduleResults.site2,
                        },
                    };
                    assert.ifError(err);
                    assert.deepStrictEqual(res, expectedResults);
                    done();
                }, { backbeat: { host: 'localhost', port: testPort } });
            });
        });
    });
});

describe('reportHanlder::getIngestionInfo', function testSuite() {
    this.timeout(20000);
    const testPort = '4242';
    let httpServer;

    describe('Test Request Success Cases', () => {
        before(done => {
            httpServer = http.createServer(requestHandler)
                             .listen(testPort);
            httpServer.on('listening', done);
            httpServer.on('error', err => {
                process.stdout.write(`https server: ${err.stack}\n`);
                process.exit(1);
            });
        });

        after('Terminating Server', () => {
            httpServer.close();
        });

        it('should return correct results', done => {
            getIngestionInfo(logger, (err, res) => {
                const expectedStatusResults = {
                    states: {
                        site1: 'enabled',
                        site2: 'disabled',
                    },
                    schedules: {
                        site2: expectedScheduleResults.site2,
                    },
                };
                assert.ifError(err);

                assert(res.metrics);
                assert(res.status);
                assert.deepStrictEqual(res.status, expectedStatusResults);
                assert.deepStrictEqual(res.metrics,
                    ingestionExpectedResultsRef);
                done();
            }, config);
        });

        it('should return empty if no ingestion locations exist', done => {
            getIngestionInfo(logger, (err, res) => {
                assert.ifError(err);

                assert(res.metrics);
                assert(res.status);
                assert.deepStrictEqual(res.metrics, {});
                assert.deepStrictEqual(res.status, {});
                done();
            });
        });
    });
});
