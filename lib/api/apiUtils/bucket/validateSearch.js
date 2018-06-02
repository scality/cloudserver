const Parser = require('sql-where-parser');
const { errors } = require('arsenal');
const objModel = require('arsenal').models.ObjectMD;

const BINARY_OP = 2;
const sqlConfig = {
    operators: [
        {
            '=': BINARY_OP,
            '<': BINARY_OP,
            '>': BINARY_OP,
            '<>': BINARY_OP,
            '<=': BINARY_OP,
            '>=': BINARY_OP,
            '!=': BINARY_OP,
        },
        { LIKE: BINARY_OP },
        { AND: BINARY_OP },
        { OR: BINARY_OP },
    ],
    tokenizer: {
        shouldTokenize: ['(', ')', '=', '!=', '<', '>', '<=', '>=', '<>'],
        shouldMatch: ['"', '\'', '`'],
        shouldDelimitBy: [' ', '\n', '\r', '\t'],
    },
};
const parser = new Parser(sqlConfig);

function _validateTree(whereClause, possibleAttributes) {
    let invalidAttribute;

    function _searchTree(node) {
        if (typeof node !== 'object') {
            invalidAttribute = node;
        } else {
            const operator = Object.keys(node)[0];
            if (operator === 'AND' || operator === 'OR') {
                _searchTree(node[operator][0]);
                _searchTree(node[operator][1]);
            } else {
                const field = node[operator][0];
                if (!field.startsWith('tags.') &&
                    !possibleAttributes[field] &&
                    !field.startsWith('x-amz-meta-')) {
                    invalidAttribute = field;
                }
            }
        }
    }
    _searchTree(whereClause);
    return invalidAttribute;
}

/**
 * validateSearchParams - validate value of ?search= in request
 * @param  {string} searchParams - value of search params in request
 *  which should be jsu sql where clause
 * For metadata: x-amz-meta-color=\"blue\"
 * For tags: tags.x-amz-meta-color=\"blue\"
 * For any other attribute: `content-length`=5
 * @return {undefined | error} undefined if validates or arsenal error if not
 */
function validateSearchParams(searchParams) {
    let ast;
    try {
        ast = parser.parse(searchParams);
    } catch (e) {
        if (e) {
            return {
                error: errors.InvalidArgument
                .customizeDescription('Invalid sql where clause ' +
                'sent as search query'),
            };
        }
    }
    const possibleAttributes = objModel.getAttributes();
    const invalidAttribute = _validateTree(ast, possibleAttributes);
    if (invalidAttribute) {
        return {
            error: errors.InvalidArgument
                .customizeDescription('Search param ' +
                `contains unknown attribute: ${invalidAttribute}`) };
    }
    return {
        ast,
    };
}

module.exports = validateSearchParams;
