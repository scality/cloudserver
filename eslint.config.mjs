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

        rules: {
            'max-len': 'off',
            'promise/prefer-await-to-then': 'warn',
            'n/callback-return': 'warn',
        },
    },
    {
        files: ['**/*.js'],
        languageOptions: {
            sourceType: 'commonjs',
        },
    },
    mocha(['tests/**/*.js']),
];
