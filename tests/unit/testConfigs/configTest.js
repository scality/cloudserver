const assert = require('assert');
const utils = require('../../../lib/data/external/utils');

const results = [
  { sourceLocationConstraintName: 'azuretest',
    destLocationConstraintName: 'azuretest',
    boolExpected: true,
  },
  { sourceLocationConstraintName: 'azuretest2',
    destLocationConstraintName: 'azuretest2',
    boolExpected: true,
  },
  { sourceLocationConstraintName: 'aws-test',
    destLocationConstraintName: 'aws-test',
    boolExpected: true,
  },
  { sourceLocationConstraintName: 'aws-test',
    destLocationConstraintName: 'aws-test-2',
    boolExpected: true,
  },
  { sourceLocationConstraintName: 'aws-test-2',
    destLocationConstraintName: 'aws-test-2',
    boolExpected: true,
  },
  { sourceLocationConstraintName: 'mem-test',
    destLocationConstraintName: 'mem-test',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'mem-test',
    destLocationConstraintName: 'azuretest',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'azuretest',
    destLocationConstraintName: 'mem-test',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'aws-test',
    destLocationConstraintName: 'mem-test',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'mem-test',
    destLocationConstraintName: 'aws-test',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'azuretest',
    destLocationConstraintName: 'aws-test',
    boolExpected: false,
  },
  { sourceLocationConstraintName: 'azuretest',
    destLocationConstraintName: 'azuretest2',
    boolExpected: false,
  },
];

describe('Testing Config.js function: ', () => {
    results.forEach(result => {
        it(`should return ${result.boolExpected} if source location ` +
        `constriant name equals to ${result.sourceLocationConstraintName} ` +
        'destination location constraint equals to' +
        `and ${result.destLocationConstraintName}`, done => {
            const isCopy = utils.externalBackendCopy(
              result.sourceLocationConstraintName,
              result.destLocationConstraintName);
            assert.strictEqual(isCopy, result.boolExpected);
            done();
        });
    });
});
