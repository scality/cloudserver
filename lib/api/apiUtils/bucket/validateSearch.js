const Parser = require('sql-where-parser');
const parser = new Parser();
const { errors } = require('arsenal');
const objModel = require('arsenal').models.ObjectMD;

function _validateTree(whereClause, possibleAttributes) {
    let invalidAttribute;

    function _searchTree(node) {
        const operator = Object.keys(node)[0];

        if (operator === 'AND') {
            _searchTree(node[operator][0]);
            _searchTree(node[operator][1]);
        } else if (operator === 'OR') {
            _searchTree(node[operator][0]);
            _searchTree(node[operator][1]);
        } else {
            const field = node[operator][0];

            if (!possibleAttributes[field] &&
                !field.startsWith('x-amz-meta-')) {
                invalidAttribute = field;
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
 * For metadata: userMd.`x-amz-meta-color`=\"blue\"
 * For tags: tags.`x-amz-meta-color`=\"blue\"
 * For any other attribute: `content-length`=5
 * @return {undefined | error} undefined if validates or arsenal error if not
 */
function validateSearchParams(searchParams) {
    let ast;
    try {
        ast = parser.parse(searchParams, (operator, operands) => {
            switch (operator) {
            case '-':
                return `${operands[0]}-${operands[1]}`;
            default:
                return parser.defaultEvaluator(operator, operands);
            }
        });
    } catch (e) {
        if (e) {
            return errors.InvalidArgument
            .customizeDescription('Invalid sql where clause ' +
            'sent as search query');
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
