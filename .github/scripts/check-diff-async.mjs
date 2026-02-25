/**
 * Check that all new/modified functions in the current git diff use async/await.
 * Fails with exit code 1 if any additions introduce callback-style functions or .then() chains.
 *
 * Usage: node scripts/check-diff-async.mjs
 * In CI: runs against the current PR diff (files changed vs base branch)
 */
import { execSync } from 'node:child_process';
import { Project, SyntaxKind } from 'ts-morph';

const CALLBACK_PARAM_PATTERN = /^(cb|callback|next|done|err)$/i;

function getChangedJsFiles() {
    const base = process.env.GITHUB_BASE_REF
        ? `origin/${process.env.GITHUB_BASE_REF}`
        : 'HEAD';
    const output = execSync(`git diff --name-only --diff-filter=ACMR ${base} -- '*.js'`, {
        encoding: 'utf8',
    }).trim();

    return output ? output.split('\n').filter(f => f.endsWith('.js')) : [];
}

/**
 * Get added line numbers for a file in the current diff.
 */
function getAddedLineNumbers(filePath) {
    const base = process.env.GITHUB_BASE_REF
        ? `origin/${process.env.GITHUB_BASE_REF}`
        : 'HEAD';
    const diff = execSync(`git diff ${base} -- ${filePath}`, { encoding: 'utf8' });
    const addedLines = new Set();
    let currentLine = 0;

    for (const line of diff.split('\n')) {
        const hunkMatch = line.match(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/);

        if (hunkMatch) {
            currentLine = parseInt(hunkMatch[1], 10) - 1;
            continue;
        }

        if (line.startsWith('+') && !line.startsWith('+++')) {
            currentLine++;
            addedLines.add(currentLine);
        } else if (!line.startsWith('-')) {
            currentLine++;
        }
    }

    return addedLines;
}

const changedFiles = getChangedJsFiles();
if (changedFiles.length === 0) {
    console.log('No changed JS files to check.');
    process.exit(0);
}

console.log(`Checking ${changedFiles.length} changed JS file(s) for async/await compliance...\n`);

const project = new Project({
    compilerOptions: { allowJs: true, noEmit: true },
    skipAddingFilesFromTsConfig: true,
});

const filesToCheck = changedFiles.filter(f =>
    !f.startsWith('tests/') &&
    !f.startsWith('node_modules/') &&
    (
        f.startsWith('lib/') ||
        f.startsWith('bin/') ||
        !f.includes('/')
    )
);
if (filesToCheck.length === 0) {
    console.log('No source JS files in diff (tests and node_modules excluded).');
    process.exit(0);
}

project.addSourceFilesAtPaths(filesToCheck);

const violations = [];

for (const sourceFile of project.getSourceFiles()) {
    const filePath = sourceFile.getFilePath().replace(process.cwd() + '/', '');
    const addedLines = getAddedLineNumbers(filePath);

    if (addedLines.size === 0) continue;

    const functions = [
        ...sourceFile.getDescendantsOfKind(SyntaxKind.FunctionDeclaration),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.FunctionExpression),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.ArrowFunction),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.MethodDeclaration),
    ];

    for (const fn of functions) {
        if (fn.isAsync()) continue;

        const startLine = fn.getStartLineNumber();
        if (!addedLines.has(startLine)) continue;

        const params = fn.getParameters();
        const lastParam = params[params.length - 1];
        if (lastParam && CALLBACK_PARAM_PATTERN.test(lastParam.getName())) {
            violations.push({
                file: filePath,
                line: startLine,
                type: 'callback',
                detail: `function has callback parameter '${lastParam.getName()}'`,
            });
        }
    }

    const propertyAccesses = sourceFile.getDescendantsOfKind(SyntaxKind.PropertyAccessExpression);
    for (const access of propertyAccesses) {
        if (access.getName() !== 'then') continue;
        const line = access.getStartLineNumber();
        if (addedLines.has(line)) {
            violations.push({
                file: filePath,
                line,
                type: 'then-chain',
                detail: 'use await instead of .then()',
            });
        }
    }
}

if (violations.length === 0) {
    console.log('✓ All new code in the diff uses async/await.');
    process.exit(0);
}

console.error(`✗ Found ${violations.length} async/await violation(s) in the diff:\n`);
for (const v of violations) {
    console.error(`  ${v.file}:${v.line} [${v.type}] ${v.detail}`);
}
console.error('\nNew code must use async/await instead of callbacks or .then() chains.');
console.error('See the async/await migration guide in CONTRIBUTING.md for help.');
process.exit(1);
