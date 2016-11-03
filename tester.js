import fs from 'fs';
import cp from 'child_process';
import process from 'process';
import path from 'path';

const rootPath = path.join(__dirname, 'tests', 'functional');

function log(message) {
    return process.stdout.write(`${message}\n`);
}

function runTest(dir, fileName) {
    const testFile = path.join(dir, fileName);

    const test = cp.spawnSync('mocha', ['-t', '40000', testFile], {
        stdio: 'inherit',
        cwd: dir,
    });

    return !test.status;
}

function testing(dir) {
    let masterConfig;

    log(`testing ${dir}`);

    try {
        masterConfig = require(path.join(rootPath, dir, 'master.json'));
    } catch (err) {
        return log('master.json file not found, skipping this directory');
    }

    return masterConfig.tests.files.reduce((prev, file) => {
        const cwd = path.join(rootPath, dir);
        return prev && runTest(cwd, file);
    }, true);
}

function main() {
    log(`reading dirs in ${rootPath}`);

    const dirs = fs.readdirSync(rootPath);

    return dirs.reduce((prev, dir) => prev && testing(dir), true);
}

if (main()) {
    process.exit(0);
}
process.exit(1);
