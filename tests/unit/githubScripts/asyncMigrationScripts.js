const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const repoRoot = path.resolve(__dirname, '../../..');
const checkDiffScript = path.join(repoRoot, '.github/scripts/check-diff-async.mjs');
const countAsyncScript = path.join(repoRoot, '.github/scripts/count-async-functions.mjs');

function run(command, args, cwd, extraEnv = {}) {
    const result = spawnSync(command, args, {
        cwd,
        env: { ...process.env, ...extraEnv },
        encoding: 'utf8',
    });
    assert.strictEqual(result.status, 0, `command failed: ${command} ${args.join(' ')}\n${result.stderr}`);
    return result;
}

function initTempRepo() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudserver-script-tests-'));
    run('git', ['init'], dir);
    run('git', ['config', 'user.name', 'Copilot Test'], dir);
    run('git', ['config', 'user.email', 'copilot-test@example.com'], dir);

    fs.writeFileSync(path.join(dir, 'README.md'), 'test\n');
    run('git', ['add', 'README.md'], dir);
    run('git', ['commit', '-m', 'init'], dir);
    return dir;
}

function runNodeScript(scriptPath, cwd, extraEnv = {}) {
    return spawnSync(process.execPath, [scriptPath], {
        cwd,
        env: { ...process.env, ...extraEnv },
        encoding: 'utf8',
    });
}

describe('CI async migration scripts', () => {
    const tempDirs = [];

    after(() => {
        tempDirs.forEach(dir => fs.rmSync(dir, { recursive: true, force: true }));
    });

    it('check-diff-async exits successfully when no JS files changed', () => {
        const dir = initTempRepo();
        tempDirs.push(dir);

        const result = runNodeScript(checkDiffScript, dir);
        assert.strictEqual(result.status, 0, result.stderr);
        assert(result.stdout.includes('No changed JS files to check.'));
    });

    it('check-diff-async ignores non-source JS changes', () => {
        const dir = initTempRepo();
        tempDirs.push(dir);

        fs.mkdirSync(path.join(dir, 'tests/unit'), { recursive: true });
        fs.writeFileSync(path.join(dir, 'tests/unit/newTest.js'), 'module.exports = () => {};\n');
        run('git', ['add', 'tests/unit/newTest.js'], dir);

        const result = runNodeScript(checkDiffScript, dir);
        assert.strictEqual(result.status, 0, result.stderr);
        assert(result.stdout.includes('No source JS files in diff'));
    });

    it('check-diff-async fails on newly added callback-style function', () => {
        const dir = initTempRepo();
        tempDirs.push(dir);

        fs.mkdirSync(path.join(dir, 'lib'), { recursive: true });
        fs.writeFileSync(path.join(dir, 'lib/newFile.js'), [
            'function badStyle(param, cb) {',
            '    return cb(null, param);',
            '}',
            '',
        ].join('\n'));
        run('git', ['add', 'lib/newFile.js'], dir);

        const result = runNodeScript(checkDiffScript, dir);
        assert.strictEqual(result.status, 1);
        assert(result.stderr.includes('function has callback parameter'));
        assert(result.stderr.includes('lib/newFile.js'));
    });

    it('count-async-functions runs and prints summary', function countAsyncTest() {
        this.timeout(120000);
        const result = runNodeScript(countAsyncScript, repoRoot);

        assert.strictEqual(result.status, 0, result.stderr);
        assert(result.stdout.includes('=== Async/Await Migration Progress ==='));
        assert(result.stdout.includes('Total functions:'));
    });
});
