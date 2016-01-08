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

const avgStdGraph = `avg-std`;
const pdfCdfGraph = `pdf-cdf`;
const statSizeGraph = `stat-size`;

class Plotter {
    /**
     * @param {array} dataFiles: files storing stats: avg & std-dev
     * @param {array} funcFiles: files storing stats: estimated
     *                  probability & cumulative distribution function
     * @param {array} sizeFile: array of data sizes
     * @param {string} fileName: prefix name for .gnu and output files
     * @param {array} graphsToPlot: array of graphs for plotting
     * @return {this} Plotter
     */
    constructor(dataFiles, funcFiles, sizeFile, fileName, graphsToPlot) {
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
        this.sizes = [];
        this.reqsToTest = [];
        this.dataFiles = getArrOfString(dataFiles);
        this.funcFiles = getArrOfString(funcFiles);
        this.sizeFile = getArrOfString([sizeFile])[0];
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
        this.graphsToPlot = graphsToPlot;
        this.stats = {
            nOps: 0,
            min: [],
            max: [],
            mu: [],
            sigma: [],
            statSizes: [],
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
                this.statSizes = arr.slice(2);
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
                // `set mxtics 1\n` +
                `set grid xtics, ytics, mytics, mxtics\n` +
                `set terminal postscript enhanced color font "CMR14"\n` +
                `set output '| ps2pdf - ${this.outputFile}'\n` +
                `plot `;
            let color = 1;
            this.dataFiles.forEach((dataFile, fileIdx) => {
                let col = 1;
                const prefixTitle = dataFile.slice(dataFile.length - 15,
                                                   dataFile.length - 8);
                for (let idx = 0; idx < this.sizes.length; idx++) {
                    const title = `${prefixTitle}, size = ${this.sizes[idx]}`;
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
        function genGnuFile(genCb) {
            this.statSizes[0] = Math.floor(this.statSizes[0] / 1024);
            this.statSizes[1] = Math.ceil(this.statSizes[1] / 1024);
            const nbXtics = Math.min(5, this.statSizes[2]);
            const xtics = Math.ceil((this.statSizes[1] - this.statSizes[0]) /
                                        nbXtics);
            const mxtics = Math.min(5, Math.ceil(this.statSizes[2] / nbXtics));
            let content =
                `set key top right Left reverse box width 3 height 1.5\n` +
                `set style data linespoints\n` +
                `set xlabel 'Data sizes (KB)'\n` +
                `set ylabel 'Latency (ms): average and standard deviation'\n` +
                `set xtics ${xtics}\n` +
                `set mxtics ${mxtics}\n` +
                `set grid xtics, ytics, mytics, mxtics\n` +
                `set terminal postscript enhanced color font "CMR14"\n` +
                `set output '| ps2pdf - ${this.outputSizeFile}'\n` +
                `plot `;
            let color = 1;
            let col = 1;
            this.reqsToTest.forEach((req, idx) => {
                const title = `${reqsString[req]}`;
                content = `${content}` +
                    `"${this.sizeFile}" u ($1/1024):${col + 1} ` +
                    `notitle w lines lc ${color} lt 1 lw 2, ` +
                    `"${this.sizeFile}" u ($1/1024):${col + 1}:${col + 2} ` +
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
                    content += `set ylabel "size = ${size}B"\n`;
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
                                `"{/Symbol m} = ${this.stats.mu[col - 2]}\\n` +
                                `{/Symbol s} = ${this.stats.sigma[col - 2]}" ` +
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
                            `title 'size = ${size}B' ` +
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
                this.createGnuFilePdfCdf(cb);
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
            if (this.graphsToPlot === undefined) {
                cmd += `gnuplot ${this.gnuFile}; `;
                this.gnuPdfCdf.forEach(file => {
                    cmd += `gnuplot ${file}; `;
                });
                cmd += `gnuplot ${this.gnuSizeFile}`;
            } else if (this.graphsToPlot.indexOf(avgStdGraph) > -1) {
                cmd += `gnuplot ${this.gnuFile}; `;
            } else if (this.graphsToPlot.indexOf(pdfCdfGraph) > -1) {
                this.gnuPdfCdf.forEach(file => {
                    cmd += `gnuplot ${file}; `;
                });
            } else if (this.graphsToPlot.indexOf(statSizeGraph) > -1) {
                cmd += `gnuplot ${this.gnuSizeFile}`;
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
};
