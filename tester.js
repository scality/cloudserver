import fs from 'fs';
import cp from 'child_process';
import process from 'process';
import path from 'path';

const rootPath = path.join(__dirname, 'tests', 'functional');

function log(message) {
    return process.stdout.write(`${message}\n`);
}

function installDependency(dir) {
    log(`installing dependencies ${dir}`);

    try {
        fs.statSync(path.join(dir, 'package.json'));
    } catch (e) {
        return log('package.json file not found, skip installation');
    }

    try {
        cp.execSync('npm install', {
            stdio: 'inherit',
            cwd: dir,
        });
    } catch (e) {
        log('`npm install` fail');
        return false;
    }

    return true;
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
        if (!installDependency(cwd)) {
            return false;
        }

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
