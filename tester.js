import fs from 'fs';
import cp from 'child_process';
import process from 'process';
import path from 'path';

const rootPath = path.join(__dirname, 'tests', 'functional');

function log(message) {
    return process.stdout.write(`${message}\n`);
}

function testing(dir) {
    log(`testing ${dir}`);

    try {
        fs.statSync(path.join(rootPath, dir, 'package.json'));
    } catch (e) {
        return log('package.json file not found, skipping this directory');
    }

    const test = cp.spawnSync('npm', ['test'], {
        stdio: 'inherit',
        cwd: path.join(rootPath, dir),
    });

    return !test.status;
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
