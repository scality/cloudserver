'use strict'; // eslint-disable-line strict

const Plotter = require('./plotter');
const S3Blaster = require('./s3blaster');

let plotter = undefined;
let blaster = undefined;

const host = 'nodea.ringr2.devsca.com';
const port = 80;
blaster = new S3Blaster(host, port);
if (blaster === undefined) {
    process.exit('Failed to create S3Blaster');
}

const PUT_OBJ = S3Blaster.requests.putObj;
const GET_OBJ = S3Blaster.requests.getObj;
const DEL_OBJ = S3Blaster.requests.delObj;
const COM_OBJ = S3Blaster.requests.comObj;
const LST_OBJ = S3Blaster.requests.lstObj;

const simulEach = S3Blaster.simulPolicy.each;
const simulMixed = S3Blaster.simulPolicy.mixed;

const statsFolder = S3Blaster.statsFolder.path;

/* Available graph to be plotted:
graphs = {
   avgStd: average and standard-deviabtion graph will be plotted
   pdfCdf: estimated pdf/cdf graphs will be plotted
   statSize: latency vs. sizes graph will be plotted
   thread: latency vs. number of threads graph will be plotted
};
*/
const graphs = Plotter.graphs;

const defaultFileName = statsFolder +
    process.argv[2].slice(process.argv[2].lastIndexOf('/'),
                          process.argv[2].lastIndexOf('.'));

function doAfterTest(blaster, plotter, cb) {
    blaster.updateStatsFiles(err => {
        if (err) {
            return cb(err);
        }
        return plotter.plotData(err => {
            if (err) {
                process.stdout.write(err);
            }
            blaster.clearDataSimul(err => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    });
}

function genArray(min, max, step) {
    const arr = [];
    let size = min;
    do {
        arr.push(size);
        size = size + step;
    } while (size <= max);
    return arr;
}
/*
 * Explaination functions setting parameters of S3Blaster
 * nbWorkers: number of workers
 * nbBuckets: number of buckets (per process/worker)
 * prefSufName: prefix and suffix name for data files and output files
 * reqsToTest: array of requests to be testinng. `reqsToTest` should be
 *             a subset of [PUT_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ]
 * resetStatsAfterEachTest:
 *   -> if it is true, stats will be reset after each `it` test
 *   -> otherwise, stats is commulative over all `it`s of each `describe` test
 * simulPolicy:
 *   -> there are two simulation policies that schedules testing of requests:
 *    (1) simulEach: in each `it` test, request is tested one after one.
 *          For each request, data size is tested one after one.
 *    (2) simulMixed: in each `it` test, request and data size are chosen at
 *         random for testing.
 * freqsToShow:
 *   -> stats will be shown to console and stored (to be written to data files)
 *      after each `freqsToShow` number of iterations. If it is set to be -1,
 *      a default value will be applied.
 * nOps: number of operations per individual request. If it is set to be -1,
 *      number of operations given by commandline will be used.
 * sizes: array of data sizes for tests
 * distrFuncParams: [step, samplesNb] for estimating pdf and cdf
 *   -> step: step between two samples
 *   -> samplesNb: number of samples
 * arrThreads: array of number of threads for tests
 * NOTE: a setting from previous describse test will be saved unless it is
 * re-defined by current describe test.
 */

/* A note for Plotter: we can define which graph to be plotter by setting
 *  `graphsToPlot`: array of graphs for plotting. Available graphs are:
 *   - graphs.avgStd: average and standard-deviabtion graph will be plotted
 *   - graphs.pdfCdf: estimated pdf/cdf graphs will be plotted
 *   - graphs.statSize: latency vs. sizes graph will be plotted
 *   - graphs.thread: latency vs. number of threads graph will be plotted
 * If `graphsToPlot` is note defined, all the graphs will be plotted.
 */

const KB = 1024;
const MB = KB * KB;
/* example parameters of S3Blaster */
const params = {
    nbBuckets: 10,
    prefSufName: [defaultFileName, ''],
    reqsToTest: [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ],
    resetStatsAfterEachTest: false,
    simulPolicy: simulEach,
    freqsToShow: -1,
    nOps: -1,
    sizes: genArray(KB, 200 * KB, 10 * KB),
    distrFuncParams: [0.2, 2000],
};
blaster.setParams(params);

const sizesToTest = genArray(10 * KB, MB, 50 * KB);
const largeSizes = [16 * MB, 64 * MB, 256 * MB];
const threadsToTest = genArray(1, 100, 1);

describe('Measure perf. vs. threads with 10 buckets, 1 worker', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Thread_10Bkts_1Wrk`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.thread, graphs.pdfCdf];
    before(done => {
        blaster.setParams({
            nbWorkers: 1,
            nbBuckets: 10,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 1000,
            freqsToShow: 1000,
            sizes: [KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [0.5, 2000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. vs.threads with 100 buckets, 1 worker', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Thread_100Bkts_1Wrk`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.thread, graphs.pdfCdf];
    before(done => {
        blaster.setParams({
            nbWorkers: 1,
            nbBuckets: 1000,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 100000,
            freqsToShow: 1000,
            sizes: [KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [0.5, 2000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. vs.threads with 10 buckets, 10 workers', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Thread_10Bkts_10Wrks`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.thread, graphs.pdfCdf];
    before(done => {
        blaster.setParams({
            nbWorkers: 10,
            nbBuckets: 10,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 1000,
            freqsToShow: 1000,
            sizes: [KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [0.5, 2000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf.vs.threads with 100 buckets, 10workers', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Thread_100Bkts_10Wrks`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.thread, graphs.pdfCdf];
    before(done => {
        blaster.setParams({
            nbWorkers: 10,
            nbBuckets: 10,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 100000,
            freqsToShow: 1000,
            sizes: [KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [0.5, 2000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. vs. sizes with 10 buckets, 1 worker', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Size_10Bkts_1Wrk`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.statSize];
    before(done => {
        blaster.setParams({
            nbWorkers: 1,
            nbBuckets: 10,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 1000,
            freqsToShow: 1000,
            sizes: sizesToTest,
            arrThreads: [1, 10, 60],
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. vs. sizes with 100 buckets, 1 worker', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Size_100Bkts_1Wrk`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.statSize];
    before(done => {
        blaster.setParams({
            nbWorkers: 1,
            nbBuckets: 100,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 100000,
            freqsToShow: 100000,
            sizes: sizesToTest,
            arrThreads: [1, 10, 60],
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. vs. sizes with 10 buckets, 10 worker', function fn() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Size_10Bkts_10Wrks`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.statSize];
    before(done => {
        blaster.setParams({
            nbWorkers: 10,
            nbBuckets: 10,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 100000,
            freqsToShow: 100000,
            sizes: sizesToTest,
            arrThreads: [1, 10, 60],
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            return done();
        });
    });

    it('Only PUT', done => {
        blaster.setActions([PUT_OBJ]);
        blaster.doSimul(done);
    });

    it('Only LIST', done => {
        blaster.setActions([LST_OBJ]);
        blaster.doSimul(done);
    });

    it('Only GET', done => {
        blaster.setActions([GET_OBJ]);
        blaster.doSimul(done);
    });

    it('Only DELETE', done => {
        blaster.setActions([DEL_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure perf. of large sizes, 1 bkt, 10 workers', function fc() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}LargeSizes_10Wrks`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ];
    before(done => {
        blaster.setParams({
            nbWorkers: 10,
            nbBuckets: 1,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 10000,
            freqsToShow: 10000,
            sizes: largeSizes,
            arrThreads: [1, 10, 60],
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            return done();
        });
    });

    it('Only COMBINATON', done => {
        blaster.setActions([COM_OBJ]);
        blaster.doSimul(done);
    });

    afterEach(done => {
        blaster.updateDataFiles(done);
    });

    after(done => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure mixed perf.', function mixedPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Mixed`, ''];
    const _reqsToTest = [PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ];
    before(done => {
        blaster.setParams({
            nbWorkers: 10,
            nbBuckets: 100,
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulMixed,
            nOps: 10000,
            freqsToShow: 10000,
            sizes: sizesToTest,
            arrThreads: [1, 10, 60],
            distrFuncParams: [0.5, 2000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            return done();
        });
    });

    it('PUT <-> LIST <-> GET <-> DELETE <-> PUT', done => {
        blaster.setActions([PUT_OBJ, LST_OBJ, GET_OBJ, DEL_OBJ]);
        blaster.doSimul(done);
    });

    after(done => {
        blaster.updateDataFiles(err => {
            if (err) {
                return done(err);
            }
            return doAfterTest(blaster, plotter, done);
        });
    });
});
