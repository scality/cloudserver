const cp = require('child_process');

const conf = require('../../../../../lib/Config').config;

const ssl = conf.https;
let transportArgs = ['-s'];
if (ssl && ssl.ca) {
    transportArgs = ['-s', '--cacert', conf.httpsPath.ca];
}

// Get stdout and stderr stringified
function provideRawOutput(args, cb) {
    process.stdout.write(`curl ${args}\n`);
    const child = cp.spawn('curl', transportArgs.concat(args));
    const procData = {
        stdout: '',
        stderr: '',
    };
    child.stdout.on('data', data => {
        procData.stdout += data.toString();
    });
    child.on('close', () => {
        let httpCode;
        if (procData.stderr !== '') {
            const lines = procData.stderr.replace(/[<>]/g, '').split(/[\r\n]/);
            httpCode = lines.find(line => {
                const trimmed = line.trim().toUpperCase();
                // ignore 100 Continue HTTP code
                if (trimmed.startsWith('HTTP/1.1 ') &&
                    !trimmed.includes('100 CONTINUE')) {
                    return true;
                }
                return false;
            });
            if (httpCode) {
                httpCode = httpCode.trim().replace('HTTP/1.1 ', '')
                    .toUpperCase();
            }
        }
        return cb(httpCode, procData);
    });
    child.stderr.on('data', data => {
        procData.stderr += data.toString();
    });
}

module.exports = provideRawOutput;
