const assert = require('assert');

const { isLifecycleSession } =
      require('../../../../lib/api/apiUtils/authorization/permissionChecks.js');

const tests = [
    {
        arn: 'arn:aws:sts::257038443293:assumed-role/rolename/backbeat-lifecycle',
        description: 'a role assumed by lifecycle service',
        expectedResult: true,
    },
    {
        arn: undefined,
        description: 'undefined',
        expectedResult: false,
    },
    {
        arn: '',
        description: 'empty',
        expectedResult: false,
    },
    {
        arn: 'arn:aws:iam::257038443293:user/bart',
        description: 'a user',
        expectedResult: false,
    },
    {
        arn: 'arn:aws:sts::257038443293:assumed-role/rolename/other-service',
        description: 'a role assumed by another service',
        expectedResult: false,
    },
];

describe('authInfoHelper', () => {
    tests.forEach(t => {
        it(`should return ${t.expectedResult} if arn is ${t.description}`, () => {
            const result = isLifecycleSession(t.arn);
            assert.equal(result, t.expectedResult);
        });
    });
});
