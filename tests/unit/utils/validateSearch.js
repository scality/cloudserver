const assert = require('assert');
const { errors } = require('arsenal');
const validateSearch =
    require('../../../lib/api/apiUtils/bucket/validateSearch');


describe('validate search where clause', () => {
    const tests = [
        {
            it: 'should allow a valid simple search with table attribute',
            searchParams: '`x-amz-meta-dog`="labrador"',
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
            searchParams: '`x-amz-meta-dog`="labrador" ' +
            'AND `x-amz-meta-age`="5"',
            result: undefined,
        },
        {
            it: 'should allow valid search with OR',
            searchParams: '`x-amz-meta-dog`="labrador" ' +
            'OR `x-amz-meta-age`="5"',
            result: undefined,
        },
        {
            it: 'should allow valid search with double AND',
            searchParams: '`x-amz-meta-dog`="labrador" ' +
                'AND `x-amz-meta-age`="5" ' +
                'AND `x-amz-meta-whatever`="ok"',
            result: undefined,
        },
        {
            it: 'should allow valid chained search with tables and columns',
            searchParams: '`x-amz-meta-dog`="labrador" ' +
                'AND `x-amz-meta-age`="5" ' +
                'AND `content-length`="10"' +
                'OR isDeleteMarker="true"' +
                'AND `x-amz-meta-whatever`="ok"',
            result: undefined,
        },
        {
            it: 'should allow valid LIKE search',
            searchParams: '`x-amz-meta-dog` LIKE "lab%" ' +
                'AND `x-amz-meta-age` LIKE "5%" ' +
                'AND `content-length`="10"',
            result: undefined,
        },
        {
            it: 'should disallow a LIKE search with invalid attribute',
            searchParams: '`x-zma-meta-dog` LIKE "labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: x-zma-meta-dog'),
        },
        {
            it: 'should disallow a simple search with unknown attribute',
            searchParams: '`x-zma-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: x-zma-meta-dog'),
        },
        {
            it: 'should disallow a compound search with unknown ' +
                'attribute on right',
            searchParams: '`x-amz-meta-dog`="labrador" AND ' +
                '`x-zma-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: x-zma-meta-dog'),
        },
        {
            it: 'should disallow a compound search with unknown ' +
                'attribute on left',
            searchParams: '`x-zma-meta-dog`="labrador" AND ' +
                '`x-amz-meta-dog`="labrador"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
            'param contains unknown attribute: x-zma-meta-dog'),
        },
        {
            it: 'should disallow a chained search with one invalid ' +
                'table attribute',
            searchParams: '`x-amz-meta-dog`="labrador" ' +
                'AND `x-amz-meta-age`="5" ' +
                'OR `x-zma-meta-whatever`="ok"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
                'param contains unknown attribute: x-zma-meta-whatever'),
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
            searchParams: '`x-amz-meta-dog`="labrador" ' +
                'AND `x-amz-meta-age`="5" ' +
                'OR madeUp="something"' +
                'OR `x-amz-meta-whatever`="ok"',
            result: errors.InvalidArgument.customizeDescription('Search ' +
                'param contains unknown attribute: madeUp'),
        },
        {
            it: 'should disallow unsupported query operators',
            searchParams: 'x-amz-meta-dog BETWEEN "labrador"',
            result: errors.InvalidArgument.customizeDescription(
                'Invalid sql where clause sent as search query'),
        },
    ];

    tests.forEach(test => {
        it(test.it, () => {
            const actualResult =
                  validateSearch(test.searchParams);
            if (test.result === undefined) {
                assert(typeof actualResult.ast === 'object');
            } else {
                assert.deepStrictEqual(actualResult.error, test.result);
            }
        });
    });
});
