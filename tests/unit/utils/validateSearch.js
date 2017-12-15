const assert = require('assert');
const { errors } = require('arsenal');
const validateSearch =
    require('../../../lib/api/apiUtils/bucket/validateSearch');


describe('validate search where clause', () => {
    const tests = [
        {
            it: 'should allow a valid simple search with table attribute',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador"',
            result: undefined,
        },
        {
            it: 'should allow a simple search with known ' +
                'column attribute',
            searchParams: '`content-length`="10"',
            result: undefined,
        },
        {
            it: 'should allow valid search with AND',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
            'AND userMd.`x-amz-meta-age`="5"',
            result: undefined,
        },
        {
            it: 'should allow valid search with OR',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
            'OR userMd.`x-amz-meta-age`="5"',
            result: undefined,
        },
        {
            it: 'should allow valid search with double AND',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
                'AND userMd.`x-amz-meta-age`="5" ' +
                'AND userMd.`x-amz-meta-whatever`="ok"',
            result: undefined,
        },
        {
            it: 'should allow valid chained search with tables and columns',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
                'AND userMd.`x-amz-meta-age`="5" ' +
                'AND `content-length`="10"' +
                'OR isDeleteMarker="true"' +
                'AND userMd.`x-amz-meta-whatever`="ok"',
            result: undefined,
        },
        {
            it: 'should allow valid LIKE search',
            searchParams: 'userMd.`x-amz-meta-dog` LIKE "lab%" ' +
                'AND userMd.`x-amz-meta-age` LIKE "5%" ' +
                'AND `content-length`="10"',
            result: undefined,
        },
        {
            it: 'should disallow a LIKE search with invalid attribute',
            searchParams: 'userNotMd.`x-amz-meta-dog` LIKE "labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: userNotMd'),
        },
        {
            it: 'should disallow a simple search with unknown attribute',
            searchParams: 'userNotMd.`x-amz-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: userNotMd'),
        },
        {
            it: 'should disallow a compound search with unknown ' +
                'attribute on right',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" AND ' +
                'userNotMd.`x-amz-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: userNotMd'),
        },
        {
            it: 'should disallow a compound search with unknown ' +
                'attribute on left',
            searchParams: 'userNotMd.`x-amz-meta-dog`="labrador" AND ' +
                'userMd.`x-amz-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: userNotMd'),
        },
        {
            it: 'should disallow a chained search with one invalid ' +
                'table attribute',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
                'AND userMd.`x-amz-meta-age`="5" ' +
                'OR userNotMd.`x-amz-meta-whatever`="ok"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
                'param contains unknown attribute: userNotMd'),
        },
        {
            it: 'should disallow a simple search with unknown ' +
                'column attribute',
            searchParams: 'whatever="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: whatever'),
        },
        {
            it: 'should disallow a chained search with one invalid ' +
                'column attribute',
            searchParams: 'userMd.`x-amz-meta-dog`="labrador" ' +
                'AND userMd.`x-amz-meta-age`="5" ' +
                'OR madeUp="something"' +
                'OR userMd.`x-amz-meta-whatever`="ok"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
                'param contains unknown attribute: madeUp'),
        },
    ];

    tests.forEach(test => {
        it(test.it, () => {
            const actualResult =
                validateSearch(test.searchParams);
            assert.deepStrictEqual(actualResult, test.result);
        });
    });
});
