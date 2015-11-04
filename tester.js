'use strict'; // eslint-disable-line strict

const fs = require('fs');
const proc = require('child_process');
const process = require('process');

const testDir = `${__dirname}/tests/functional`;

function testing(dir) {
    process.stdout.write(`testing ${dir}\n`);
    let master;
    try {
        master = fs.readFileSync(`${testDir}/${dir}/master.json`);
    } catch (err) {
        process.stdout.write('master.json file not found, '
                             + 'skipping this directory\n');
        return true;
    }
    const config = JSON.parse(master.toString());
    return config.tests.files.reduce((prev, file) => {
        const test = proc.spawnSync('mocha',
                [ `${testDir}/${dir}/${file}` ], {stdio: 'inherit'});
        return prev && !test.status;
    }, true);
}

function main() {
    process.stdout.write(`reading dirs in ${__dirname}/tests/functional\n`);
    const dirs = fs.readdirSync(testDir);
    return dirs.reduce((prev, dir) => {
        return prev && testing(dir);
    }, true);
}

main() ? process.exit(0) : process.exit(1);
