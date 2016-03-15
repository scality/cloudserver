'use strict'; // eslint-disable-line strict

const Plotter = require('./plotter');
const S3Blaster = require('./s3blaster');

let plotter = undefined;
let blaster = undefined;

blaster = new S3Blaster();
if (blaster === undefined) {
    process.exit('Failed to create S3Blaster');
}

const PUT_OBJ = S3Blaster.requests.putObj;
const GET_OBJ = S3Blaster.requests.getObj;
const DEL_OBJ = S3Blaster.requests.delObj;
const COM_OBJ = S3Blaster.requests.comObj;

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
    blaster.updateStatsFiles((err) => {
        if (err) {
            return cb(err);
        }
        plotter.plotData((err) => {
            if (err) {
                process.stdout.write(err);
            }
            blaster.clearDataSimul((err) => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    });
}

function genArrSizes(min, max, step) {
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
    prefSufName: [defaultFileName, ''],
    reqsToTest: [PUT_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ],
    resetStatsAfterEachTest: false,
    simulPolicy: simulEach,
    freqsToShow: -1,
    nOps: -1,
    sizes: genArrSizes(KB, 200 * KB, 10 * KB),
    distrFuncParams: [0.2, 2000],
};
blaster.setParams(params);

const sizesToTest = genArrSizes(KB, MB, 100 * KB).
                        concat(genArrSizes(2 * MB, 10 * MB, MB));

const sizesToPut = genArrSizes(10 * KB, 200 * KB, 10 * KB);

const threadsToTest = genArrSizes(1, 60, 1);

describe('Measure individual PUT vs. threads', function indivPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}PutOnlyThread`, ''];
    const _reqsToTest = [PUT_OBJ];
    const graphsToPlot = [graphs.thread];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: 200,
            freqsToShow: -1,
            sizes: [KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [1, 3000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            done();
        });
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure individual PUT vs. sizes', function indivPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}PutOnlySize`, ''];
    const _reqsToTest = [PUT_OBJ];
    const graphsToPlot = [graphs.statSize];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: -1,
            freqsToShow: -1,
            sizes: sizesToPut,
            arrThreads: -1,
            distrFuncParams: [1, 3000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            done();
        });
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure individual PUT/GET/DELETE vs. sizes', function indivPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Size`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.statSize];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: -1,
            freqsToShow: -1,
            sizes: sizesToPut,
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            done();
        });
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    it('Only GET', (done) => {
        blaster.setActions([false, true]);
        blaster.doSimul(done);
    });

    it('Only DELETE', (done) => {
        blaster.setActions([false, false, true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure individual PUT/GET/DELETE vs. threads', function indivPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Thread`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ];
    const graphsToPlot = [graphs.thread];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: -1,
            freqsToShow: -1,
            sizes: [KB, 100 * KB, MB],
            arrThreads: threadsToTest,
            distrFuncParams: [1, 5000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0], graphsToPlot);
            done();
        });
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    it('Only GET', (done) => {
        blaster.setActions([false, true]);
        blaster.doSimul(done);
    });

    it('Only DELETE', (done) => {
        blaster.setActions([false, false, true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure individual PUT/GET/DELETE', function indivPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Each`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
            nOps: -1,
            freqsToShow: -1,
            sizes: sizesToTest,
            threads: -1,
            distrFuncParams: [2, 3000],
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            done();
        });
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    it('Only GET', (done) => {
        blaster.setActions([false, true]);
        blaster.doSimul(done);
    });

    it('Only DELETE', (done) => {
        blaster.setActions([false, false, true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure combined request PUT->GET->DELETE', function combPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Comb`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            done();
        });
    });

    it('Only COMBINATON', (done) => {
        blaster.setActions([false, false, false, true]);
        blaster.doSimul(done);
    });

    afterEach((done) => {
        blaster.updateDataFiles(done);
    });

    after((done) => {
        doAfterTest(blaster, plotter, done);
    });
});

describe('Measure mixed PUT/GET/DELETE', function mixedPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Mixed`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulMixed,
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            done();
        });
    });

    it('PUT <-> GET <-> DELETE <-> PUT', (done) => {
        blaster.setActions([true, true, true]);
        blaster.doSimul(done);
    });

    after((done) => {
        blaster.updateDataFiles((err) => {
            if (err) {
                return done(err);
            }
            doAfterTest(blaster, plotter, done);
        });
    });
});

describe('Measure serial PUT/GET/DELETE', function serialPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Serial`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            done();
        });
    });

    it('PUT -> GET', (done) => {
        blaster.setActions([true, true]);
        blaster.doSimul(done);
    });

    it('GET -> DELETE', (done) => {
        blaster.setActions([false, true, true]);
        blaster.doSimul(done);
    });

    it('PUT -> DELETE', (done) => {
        blaster.setActions([true, false, true]);
        blaster.doSimul(done);
    });

    it('PUT -> GET -> DELETE', (done) => {
        blaster.setActions([true, true, true]);
        blaster.doSimul(done);
    });

    after((done) => {
        blaster.updateDataFiles((err) => {
            if (err) {
                return done(err);
            }
            doAfterTest(blaster, plotter, done);
        });
    });
});

describe('Measure personalized PUT GET DELETE', function perPerf() {
    this.timeout(0);
    const _prefSufName = [`${defaultFileName}Personalized`, ''];
    const _reqsToTest = [PUT_OBJ, GET_OBJ, DEL_OBJ, COM_OBJ];
    before((done) => {
        blaster.setParams({
            prefSufName: _prefSufName,
            reqsToTest: _reqsToTest,
            simulPolicy: simulEach,
        });
        blaster.init((err, arrDataFiles) => {
            if (err) {
                return done(err);
            }
            plotter = new Plotter(arrDataFiles, _prefSufName[0]);
            done();
        });
    });

    it('PUT -> GET', (done) => {
        blaster.setActions([true, true]);
        blaster.doSimul(done);
    });

    it('Mixed PUT <-> GET <-> DELETE <-> PUT', (done) => {
        blaster.setSimulPolicy(simulMixed);
        blaster.setActions([true, true, true]);
        blaster.doSimul(done);
    });

    it('Only PUT', (done) => {
        blaster.setSimulPolicy(simulEach);
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    it('Only COMBINATON', (done) => {
        blaster.setActions([false, false, false, true]);
        blaster.doSimul(done);
    });

    it('PUT -> GET -> DELETE', (done) => {
        blaster.setActions([true, true, true]);
        blaster.doSimul(done);
    });

    it('Only PUT', (done) => {
        blaster.setActions([true]);
        blaster.doSimul(done);
    });

    after((done) => {
        blaster.updateDataFiles((err) => {
            if (err) {
                return done(err);
            }
            doAfterTest(blaster, plotter, done);
        });
    });
});
