const { policies } = require('arsenal');
const metadata = require('./metadata/wrapper');

const { convertConditionOperator } = policies.conditions;

function tagConditionKeyAuth(authorizationResults, request, log, cb) {
    for (let i = 0; i < authorizationResults.length; i++) {
        if (!authorizationResults[i].tagConditions) {
            return cb({ isAllowed: true });
        }
        // assume authorizationResults will contain any statement containting tag condition key
        if (authorizationResults[i].tagConditions.includes())
    }
}

module.exports = tagConditionKeyAuth;