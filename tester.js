import fs from 'fs';
import cp from 'child_process';
import process from 'process';
import path from 'path';

const rootPath = path.join(__dirname, 'tests', 'functional');

function testing(dir) {
    let masterConfig;

    process.stdout.write(`testing ${dir}\n`);

    try {
        masterConfig = require(path.join(rootPath, dir, 'master.json'));
    } catch (err) {
        return process.stdout.write(['master.json file not found',
            'skipping this directory'].join('\n'));
    }

    return masterConfig.tests.files.reduce((prev, file) => {
        const cwd = path.join(rootPath, dir);
        const testFile = path.join(cwd, file);

        const test = cp.spawnSync('mocha', [testFile], {
            stdio: 'inherit',
            cwd
        });

        return prev && !test.status;
    }, true);
}

function main() {
    process.stdout.write(`reading dirs in ${rootPath}\n`);

    const dirs = fs.readdirSync(rootPath);

    return dirs.reduce((prev, dir) => {
        return prev && testing(dir);
    }, true);
}

main() ? process.exit(0) : process.exit(1);
