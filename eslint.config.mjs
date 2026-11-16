import promise from 'eslint-plugin-promise';
import n from 'eslint-plugin-n';
import scality from '@scality/eslint-config-scality';
import { mocha } from '@scality/eslint-config-scality/testing';

export default [
    ...scality,
    {
        plugins: {
            promise,
            n,
        },

        languageOptions: {
            ecmaVersion: 2021,
            sourceType: 'script',
        },

        rules: {
            'no-useless-escape': 'off',
            'max-len': 'off',
            'no-redeclare': ['error', { builtinGlobals: false }],
            'no-useless-assignment': 'off',
            'no-unassigned-vars': 'off',
            'preserve-caught-error': 'off',
            'promise/prefer-await-to-then': 'warn',
            'n/callback-return': 'warn',
        },
    },
    mocha(['tests/**/*.js']),
];
