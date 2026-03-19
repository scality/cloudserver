/**
 * Count async vs callback-style functions across the codebase using ts-morph.
 * Used in CI to track async/await migration progress over time.
 *
 * Usage: node scripts/count-async-functions.mjs
 */
import { readFileSync } from 'node:fs';
import { Project, SyntaxKind } from 'ts-morph';

function getSourcePathsFromPackageJson() {
    const packageJsonPath = new URL('../../package.json', import.meta.url);
    const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
    const paths = packageJson.countAsyncSourcePaths;

    if (Array.isArray(paths) && paths.length > 0 && paths.every(p => typeof p === 'string')) {
        return paths;
    }

    throw new Error('package.json must define a non-empty string array "countAsyncSourcePaths"');
}

const project = new Project({
    compilerOptions: {
        allowJs: true,
        noEmit: true,
    },
    skipAddingFilesFromTsConfig: true,
});

project.addSourceFilesAtPaths(getSourcePathsFromPackageJson());

let asyncFunctions = 0;
let totalFunctions = 0;
let callbackFunctions = 0;
let thenChains = 0;

const CALLBACK_PARAM_PATTERN = /^(cb|callback|next|done)$/i;

for (const sourceFile of project.getSourceFiles()) {
    const functions = [
        ...sourceFile.getDescendantsOfKind(SyntaxKind.FunctionDeclaration),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.FunctionExpression),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.ArrowFunction),
        ...sourceFile.getDescendantsOfKind(SyntaxKind.MethodDeclaration),
    ];

    for (const fn of functions) {
        totalFunctions++;

        if (fn.isAsync()) {
            asyncFunctions++;
            continue;
        }

        const params = fn.getParameters();
        const lastParam = params[params.length - 1];
        if (lastParam && CALLBACK_PARAM_PATTERN.test(lastParam.getName())) {
            callbackFunctions++;
        }
    }

    const propertyAccesses = sourceFile.getDescendantsOfKind(SyntaxKind.PropertyAccessExpression);
    for (const access of propertyAccesses) {
        if (access.getName() === 'then') {
            thenChains++;
        }
    }
}

const migrationPercent = totalFunctions > 0
    ? ((asyncFunctions / totalFunctions) * 100).toFixed(1)
    : '0.0';

console.log('=== Async/Await Migration Progress ===');
console.log(`Total functions:      ${totalFunctions}`);
console.log(`Async functions:      ${asyncFunctions} (${migrationPercent}%)`);
console.log(`Callback functions:   ${callbackFunctions}`);
console.log(`Remaining .then():    ${thenChains}`);
console.log('');
console.log(`Migration: ${asyncFunctions}/${totalFunctions} functions (${migrationPercent}%)`);

if (process.env.GITHUB_STEP_SUMMARY) {
    const { appendFileSync } = await import('node:fs');
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, [
        '## Async/Await Migration Progress',
        '',
        `| Metric | Count |`,
        `|--------|-------|`,
        `| Total functions | ${totalFunctions} |`,
        `| Async functions | ${asyncFunctions} (${migrationPercent}%) |`,
        `| Callback-style functions | ${callbackFunctions} |`,
        `| Remaining \`.then()\` chains | ${thenChains} |`,
        '',
    ].join('\n'));
}
