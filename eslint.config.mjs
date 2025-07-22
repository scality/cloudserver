import mocha from 'eslint-plugin-mocha';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all,
});

export default [
    ...compat.extends('@scality/scality'),
    ...compat.extends('prettier'),
    {
        plugins: {
            mocha,
        },

        languageOptions: {
            ecmaVersion: 2021,
            sourceType: 'script',
        },

        rules: {
            'no-useless-escape': 'off',
            'mocha/no-exclusive-tests': 'error',
            'no-redeclare': ['error', { builtinGlobals: false }],
        },
    },
];
