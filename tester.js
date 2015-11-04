'use strict';

const fs = require('fs');
const proc = require('child_process');

const testDir = `${__dirname}/tests/functional`;

function testing(dir) {
    console.log(`testing ${dir}`);
    let master;
    try { master = fs.readFileSync(`${testDir}/${dir}/master.json`); }
    catch (err) {
        console.log('master.json file not found, skipping this directory');
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
    console.log(`reading dirs in ${__dirname}/tests/functional`);
    const dirs = fs.readdirSync(testDir);
    return dirs.reduce((prev, dir) => {
        return prev && testing(dir);
    }, true);
}

main() ? process.exit(0) : process.exit(1);
