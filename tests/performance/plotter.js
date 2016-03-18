'use strict'; // eslint-disable-line strict

const fs = require('fs');
const readline = require('readline');
const spawn = require('child_process').spawn;
const stderr = process.stderr;

const S3Blaster = require('./s3blaster');
const reqsString = S3Blaster.requestsString.reqs;

function getArrOfString(arr) {
    if (arr !== undefined && arr.constructor === Array) {
        if (arr.every((dataFile) => {
            return (typeof dataFile === 'string');
        })) {
            return arr.slice();
        }
    }
    return undefined;
}

const KB = 1024;
const MB = KB * KB;
const GB = KB * MB;
function convertSize(size) {
    if (size < KB) {
        return `${size}B`;
    } else if (size < MB) {
        return `${size / KB}KB`;
    } else if (size < GB) {
        return `${size / MB}MB`;
    }
    return `${size / GB}GB`;
}

const avgStdGraph = `avg-std`;
const pdfCdfGraph = `pdf-cdf`;
const statSizeGraph = `stat-size`;
const threadGraph = `thread`;

class Plotter {
    /**
     * @param {array} arrDataFiles: array stores data files
     *   - files stores stats: avg & std-dev of latency
     *   - files stores stats: estimated probability & cumulative distribution
     *                          function
     *   - file stores stats: latency vs. data sizes
     *   - file stores stats: latency vs. number of threads
     * @param {string} fileName: prefix name for .gnu and output files
     * @param {array} graphsToPlot: array of graphs for plotting
     * @return {this} Plotter
     */
    constructor(arrDataFiles, fileName, graphsToPlot) {
        const gnuExt = `.gnu`;
        const outputExt = `.pdf`;
        const _fileName = fileName || `output`;
        this.gnuFile = _fileName + gnuExt;
        this.outputFile = _fileName + outputExt;
        this.gnuSizeFile = `${_fileName}_size${gnuExt}`;
        this.outputSizeFile = `${_fileName}_size${outputExt}`;
        this.gnuPdfCdf = [`${_fileName}_pdf${gnuExt}`,
                          `${_fileName}_cdf${gnuExt}`];
        this.outputPdfCdf = [`${_fileName}_pdf${outputExt}`,
                            `${_fileName}_cdf${outputExt}`];
        this.gnuThreadFile = `${_fileName}_thread${gnuExt}`;
        this.outputThreadFile = `${_fileName}_thread${outputExt}`;

        this.sizes = [];
        this.reqsToTest = [];
        this.dataFiles = getArrOfString(arrDataFiles[0]);
        this.funcFiles = getArrOfString(arrDataFiles[1]);
        this.sizeFile = getArrOfString([arrDataFiles[2]])[0];
        this.threadFile = getArrOfString([arrDataFiles[3]])[0];
        if (this.dataFiles === undefined) {
            stderr.write('missing data files for Plotter\n');
            return;
        }
        if (this.funcFiles === undefined) {
            stderr.write('missing pdf/cdf files for Plotter\n');
            return;
        }
        if (this.sizeFile === undefined) {
            stderr.write('missing size file for Plotter\n');
            return;
        }
        if (this.threadFile === undefined) {
            stderr.write('missing thread file for Plotter\n');
            return;
        }
        this.graphsToPlot = graphsToPlot;
        this.stats = {
            nOps: 0,
            min: [],
            max: [],
            mu: [],
            sigma: [],
            sizes: [],
            threads: [],
        };
    }

    /**
     * function get configuration info from stats files
     * @param {string} dataFile: path to stats file
     * @param {function} cb: callback function
     * @return {function} callback function
     */
    getConfigInfo(dataFile, cb) {
        const rl = readline.createInterface({
            input: fs.createReadStream(dataFile),
            terminal: true,
        });
        rl.on('line', line => {
            const arr = line.toString().split(" ");
            if (arr[1] === 'nOps') {
                this.stats.nOps = parseInt(arr[2], 10);
            }
            if (arr[1] === 'sizes') {
                this.sizes = arr.slice(2);
            }
            if (arr[1] === 'statSizes') {
                this.stats.sizes = arr.slice(2);
            }
            if (arr[1] === 'threads') {
                this.stats.threads = arr.slice(2);
            }
            if (arr[1] === 'requests') {
                this.reqsToTest = arr.slice(2);
            }
            if (arr[1] === 'min') {
                this.stats.min = arr.slice(2);
            }
            if (arr[1] === 'max') {
                this.stats.max = arr.slice(2);
            }
            if (arr[1] === 'mu') {
                this.stats.mu = arr.slice(2);
            }
            if (arr[1] === 'sigma') {
                this.stats.sigma = arr.slice(2);
            }
            if (arr[1] === 'End_configuration') {
                rl.close();
            }
        }).on('close', () => {
            return cb(null);
        });
    }

    /**
     * function creates .gnu files that plots graphs for average and
     *  standard deviation of request latency.
     * @param {function} cb: callback function
     * @return {function} callback
     */
    createGnuFile(cb) {
        function genGnuFile(genCb) {
            let content =
                `set key below Left reverse box width 3 height 1.5\n` +
                `set style data linespoints\n` +
                `set xlabel 'Number of operations'\n` +
                `set ylabel 'Latency (ms): average and standard deviation'\n` +
                `set grid\n` +
                `set terminal postscript enhanced color font "CMR14"\n` +
                `set output '| ps2pdf - ${this.outputFile}'\n` +
                `plot `;
            let color = 1;
            this.dataFiles.forEach((dataFile, fileIdx) => {
                let col = 1;
                const prefixTitle = dataFile.slice(dataFile.length - 15,
                                                   dataFile.length - 8);
                for (let idx = 0; idx < this.sizes.length; idx++) {
                    const title = `${prefixTitle}, ` +
                                  `size = ${convertSize(this.sizes[idx])}`;
                    content = `${content}` +
                        `"${dataFile}" u ${col}:${col + 1} ` +
                        `notitle w lines lc ${color} lt 1 lw 2, ` +
                        `"${dataFile}" u ${col}:${col + 1}:${col + 2} ` +
                        `title '${title}' w yerrorbars ` +
                        `lc ${color} lt 1 lw 1 pt ${color}`;
                    col += 3;
                    color++;
                    if (fileIdx < this.dataFiles.length - 1 ||
                        idx < this.sizes.length - 1) {
                        content += `,\\\n`;
                    }
                }
            });
            fs.writeFile(this.gnuFile, content, (err) => {
                return genCb(err);
            });
        }
        this.getConfigInfo(this.dataFiles[0], (err) => {
            if (err) return cb(err);
            genGnuFile.bind(this)(cb);
        });
    }

    /**
     * function creates .gnu files that plots graphs for request
     *  latency vs. data sizes
     * @param {function} cb: callback function
     * @return {function} callback
     */
    createGnuFileSize(cb) {
        let unit;
        let unitString;

        function genGnuFile(genCb) {
            this.stats.sizes[0] = Math.floor(this.stats.sizes[0] / unit);
            this.stats.sizes[1] = Math.ceil(this.stats.sizes[1] / unit);
            let content =
                `set key top right Left reverse box width 3 height 1.5\n` +
                `set style data linespoints\n` +
                `set xlabel 'Data sizes (${unitString})'\n` +
                `set ylabel 'Latency (ms): average and standard deviation'\n` +
                `set grid\n` +
                `set terminal postscript enhanced color font "CMR14"\n` +
                `set output '| ps2pdf - ${this.outputSizeFile}'\n` +
                `plot `;
            let color = 1;
            let col = 1;
            this.reqsToTest.forEach((req, idx) => {
                const title = `${reqsString[req]}`;
                content = `${content}` +
                    `"${this.sizeFile}" u ($1/${unit}):${col + 1} ` +
                    `notitle w lines lc ${color} lt 1 lw 2, ` +
                    `"${this.sizeFile}" u ($1/${unit}):${col + 1}:${col + 2} ` +
                    `title '${title}' w yerrorbars ` +
                    `lc ${color} lt 1 lw 1 pt ${color}`;
                col += 2;
                color++;
                if (idx < this.reqsToTest.length - 1) {
                    content += `,\\\n`;
                }
            });
            fs.writeFile(this.gnuSizeFile, content, (err) => {
                return genCb(err);
            });
        }
        this.getConfigInfo(this.sizeFile, (err) => {
            if (err) return cb(err);
            if (this.stats.sizes[0] < KB) {
                unit = 1;
                unitString = `B`;
            } else if (this.stats.sizes[0] < MB) {
                unit = KB;
                unitString = `KB`;
            } else {
                unit = MB;
                unitString = `MB`;
            }
            genGnuFile.bind(this)(cb);
        });
    }

    /**
     * function creates .gnu files that plots graphs for request
     *  latency vs. threads number
     * @param {function} cb: callback function
     * @return {function} callback
     */
    createGnuFileThread(cb) {
        function genGnuFile(genCb) {
            const nbX = this.reqsToTest.length;
            const nbY = this.sizes.length + 1;
            const layout = `${nbY},${nbX}`;
            const maxThread = this.stats.threads[this.stats.threads.length - 1];
            const minThread = this.stats.threads[0];
            const xticsNb = Math.min(this.stats.threads.length, 10);
            const xtics = Math.floor((maxThread - minThread) / xticsNb);
            const mxtics = this.stats.threads.length > 1 ?
                    this.stats.threads[1] - this.stats.threads[0] : 1;
            let color = 1;
            let col = 3;
            const step = this.sizes.length;
            let content =
                `set terminal pdfcairo size ${5 * nbX},${5 * nbY} ` +
                    `enhanced color font "CMR14, 12"\n` +
                `set output '${this.outputThreadFile}'\n` +
                `set key top left Left reverse box width 3 height 1.5\n` +
                /* plot multiple graphs
                 *   -> graphs on a column correspond to a request
                 *   -> graphs on a row correspond to a data size
                 */
                `set multiplot layout ${layout} columnsfirst ` +
                    `title "{/:Bold Latency vs. #threads, ` +
                    ` ${this.stats.nOps} requests/point}"\n` +
                `set style data linespoints\n` +
                `set ylabel 'Latency (ms): average and standard deviation'\n` +
                `set xtics ${xtics}; set mxtics ${mxtics}\n` +
                `set grid xtics mxtics ytics\n`;
            this.reqsToTest.forEach((req, reqIdx) => {
                let colorp = color;
                let firstLine = 0;
                content += `plot `;
                this.sizes.forEach((size, idx) => {
                    const title = `${reqsString[req]}`;
                    content = `${content}` +
                        `"${this.threadFile}" ` +
                        `every ${step}::${firstLine} u 1:${col} ` +
                        `notitle with linespoints lc ${colorp} lt 1 lw 2, ` +
                        `"${this.threadFile}" ` +
                        `every ${step}::${firstLine} u 1:${col}:${col + 1} ` +
                        `title '${title}, size = ${convertSize(size)}' ` +
                        `w yerrorbars lc ${colorp} lt 1 lw 1 pt ${colorp}`;
                    colorp++;
                    firstLine++;
                    if (idx < this.sizes.length - 1) {
                        content += `,\\\n`;
                    } else {
                        content += `\n`;
                    }
                });
                firstLine = 0;
                colorp = color;
                content += `\n`;
                if (reqIdx === 0) {
                    content = `${content}` +
                        `unset ylabel\n` +
                        `set ylabel 'Latency (ms): average'\n`;
                }
                content = `${content}` +
                    `a=1; b=1; c=1; d=1\n` +
                    `FIT_MAXITER = 1\n` +
                    `f(x) = a + b / (c*x + d)\n`;
                this.sizes.forEach((size, sizeIdx) => {
                    if (sizeIdx === this.sizes.length - 1) {
                        content += `set xlabel 'Number of threads'\n`;
                    }
                    const title =
                        `${reqsString[req]}, size = ${convertSize(size)}`;
                    content = `${content}` +
                        `fit f(x) "${this.threadFile}" ` +
                            `every ${step}::${firstLine} u 1:${col} ` +
                            `via a,b,c,d\n` +
                        `ti = sprintf("Estimation ` +
                            `y = %.2f+(%.2f)/(%.2fx+%.2f)", a, b, c, d)\n` +
                        `plot "${this.threadFile}" ` +
                        `every ${step}::${firstLine} u 1:${col} ` +
                        `title '${title}' ` +
                        `lc ${color} lt 1 lw 1 pt ${color},\ f(x) title ti\n`;
                    color++;
                    firstLine++;
                });
                if (reqIdx === 0) {
                    content += `unset ylabel\n`;
                }
                content += `unset xlabel\n`;
                col += 2;
                color = colorp;
            });
            content += `unset multiplot; set output\n`;
            fs.writeFile(this.gnuThreadFile, content, (err) => {
                return genCb(err);
            });
        }
        this.getConfigInfo(this.threadFile, (err) => {
            if (err) return cb(err);
            genGnuFile.bind(this)(cb);
        });
    }

    /**
     * function creates .gnu files that plots graphs of estimated
     *  pdf & cdf
     * @param {function} cb: callback function
     * @return {function} callback
     */
    createGnuFilePdfCdf(cb) {
        function genGnuFile(genCb) {
            const yLabel = [`Probability distribution function, ` +
                                `${this.stats.nOps} operations`,
                            `Cumulative distribution function, ` +
                                `${this.stats.nOps} operations`];
            const nbX = this.reqsToTest.length;
            const nbY = this.sizes.length;
            const layout = `${nbY},${nbX}`;
            let count = 0;
            this.funcFiles.forEach((dataFile, fileIdx) => {
                let content =
                    `set terminal pdfcairo size ${2 * nbX},${nbY} ` +
                            `enhanced color font "CMR14, 5"\n` +
                    `set output '${this.outputPdfCdf[fileIdx]}'\n` +
                    `set style data lines\n` +
                    `set grid xtics, ytics, mytics, mxtics\n`;
                /* plot multiple graphs
                 *   -> graphs on a column correspond to a request
                 *   -> graphs on a row correspond to a data size
                 */
                content +=
                    `set multiplot layout ${layout} ` +
                    `rowsfirst title "{/:Bold=6 ${yLabel[fileIdx]}}"\n`;
                let color = 1;
                let col = 2;
                this.sizes.forEach((size, idx) => {
                    content += `set ylabel "size = ${convertSize(size)}"\n`;
                    if (idx === this.sizes.length - 1) {
                        content += `set xlabel 'Latency (ms)'\n`;
                    }
                    this.reqsToTest.forEach((reqIdx, idxp) => {
                        if (idx === 0) {
                            content +=
                                `set title '${reqsString[reqIdx]}'\n`;
                        }
                        content +=
                            `set label ` +
                                `"avg = ${this.stats.mu[col - 2]}\\n` +
                                `std-dev = ${this.stats.sigma[col - 2]}" ` +
                                `at graph 0.8, graph 0.9 \n` +
                            `set xrange [${this.stats.min[col - 2]}:` +
                                        `${this.stats.max[col - 2]}]\n` +
                            `plot "${dataFile}" u ${1}:${col} ` +
                            `notitle lc ${color} lt 1 lw 1\n` +
                            `unset label\n`;
                        if (idxp === 0) {
                            content += `unset ylabel\n`;
                        }
                        col ++;
                        color++;
                    });
                    if (idx === 0) {
                        content += `unset title\n`;
                    }
                    if (idx === this.sizes.length - 1) {
                        content += `unset xlabel\n`;
                    }
                });
                /* plot multiple graphs: each graph on a row correspond to a
                 *   request with all data sizes
                 */
                color = 1;
                content += `set xlabel 'Latency (ms)'\n`;
                content += `set multiplot layout ${this.reqsToTest.length},1 ` +
                           `rowsfirst title "{/:Bold=6 ${yLabel[fileIdx]}}, ` +
                           `all sizes"\n`;
                this.reqsToTest.forEach((reqIdx, idxp) => {
                    col = 2 + idxp;
                    const minXReq = this.sizes.map((size, idx) => {
                        return this.stats.min[idxp +
                                              idx * this.reqsToTest.length];
                    });
                    const maxXReq = this.sizes.map((size, idx) => {
                        return this.stats.max[idxp +
                                              idx * this.reqsToTest.length];
                    });
                    const minXAllSizes = Math.min.apply(Math, minXReq);
                    const maxXAllSizes = Math.max.apply(Math, maxXReq);
                    content += `set xrange [${minXAllSizes}:${maxXAllSizes}]\n`;
                    content += `set ylabel '${reqsString[reqIdx]}'\n`;
                    content += `plot `;
                    this.sizes.forEach((size, idx) => {
                        content +=
                            `"${dataFile}" u ${1}:${col} ` +
                            `title 'size = ${convertSize(size)}' ` +
                            `lc ${color} lt 1 lw 1`;
                        if (idx < this.sizes.length - 1) {
                            content += `,\\`;
                        }
                        content += `\n`;
                        col += this.reqsToTest.length;
                        color++;
                    });
                    content += `unset ylabel\n`;
                });
                content += `unset multiplot; set output\n`;
                fs.writeFile(this.gnuPdfCdf[fileIdx], content,
                    (err) => { // eslint-disable-line
                        if (err) {
                            return genCb(err);
                        }
                        count += 1;
                        if (count === this.funcFiles.length) {
                            return genCb();
                        }
                    });
            });
        }
        this.getConfigInfo(this.funcFiles[0], (err) => {
            if (err) return cb(err);
            genGnuFile.bind(this)(cb);
        });
    }

    createAllGnuFiles(cb) {
        this.createGnuFile(err => {
            if (err) {
                return cb(err);
            }
            this.createGnuFileSize(err => {
                if (err) {
                    return cb(err);
                }
                this.createGnuFileThread(err => {
                    if (err) {
                        return cb(err);
                    }
                    this.createGnuFilePdfCdf(cb);
                });
            });
        });
    }

    plotData(cb) {
        stderr.write('plotting graphs..');
        this.createAllGnuFiles(err => {
            if (err) {
                return cb(err);
            }
            let cmd = ``;
            if (this.graphsToPlot === undefined ||
                (this.graphsToPlot.length === 0)) {
                cmd += `gnuplot ${this.gnuFile}; `;
                this.gnuPdfCdf.forEach(file => {
                    cmd += `gnuplot ${file}; `;
                });
                cmd += `gnuplot ${this.gnuSizeFile}; `;
                cmd += `gnuplot ${this.gnuThreadFile}; `;
            } else {
                this.graphsToPlot.forEach(graph => {
                    if (graph === avgStdGraph) {
                        cmd += `gnuplot ${this.gnuFile}; `;
                    } else if (graph === pdfCdfGraph) {
                        this.gnuPdfCdf.forEach(file => {
                            cmd += `gnuplot ${file}; `;
                        });
                    } else if (graph === statSizeGraph) {
                        cmd += `gnuplot ${this.gnuSizeFile}; `;
                    } else if (graph === threadGraph) {
                        cmd += `gnuplot ${this.gnuThreadFile}; `;
                    }
                });
            }

            const gnuplot = spawn('bash', ['-c', cmd]);
            gnuplot.on('exit', (err) => {
                if (err) {
                    return cb(err);
                }
                stderr.write(`done\n`);
                return cb();
            });

            gnuplot.stderr.on('data', (err) => {
                if (err) {
                    stderr.write(`gnuplot's message: ${err}\n`);
                }
            });
        });
    }
}

module.exports = Plotter;

Plotter.graphs = {
    avgStd: avgStdGraph,
    pdfCdf: pdfCdfGraph,
    statSize: statSizeGraph,
    thread: threadGraph,
};
