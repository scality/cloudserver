const Parser = require('node-sql-parser').Parser;
const parser = new Parser();
const { errors } = require('arsenal');
const objModel = require('arsenal').models.ObjectMD;

function _validateTree(whereClause, possibleAttributes) {
    let invalidAttribute;
    function _searchTree(node) {
        if (!invalidAttribute) {
            // the node.table would contain userMd for instance
            // and then the column would contain x-amz-meta-whatever
            if (node.table) {
                if (!possibleAttributes[node.table]) {
                    invalidAttribute = node.table;
                }
            }
            // if there is no table, the column would contain the non-nested
            // attribute (e.g., content-length)
            if (!node.table && node.column) {
                if (!possibleAttributes[node.column]) {
                    invalidAttribute = node.column;
                }
            }
            // the attributes we care about are always on the left because
            // the value being searched for is on the right
            if (node.left) {
                _searchTree(node.left);
            }
            if (node.right && node.right.left) {
                _searchTree(node.right.left);
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
        ast =
            parser.parse(`SELECT * FROM t WHERE ${searchParams}`);
    } catch (e) {
        if (e) {
            return errors.InvalidArgument
            .customizeDescription('Invalid sql where clause ' +
            'sent as search query');
        }
    }

    const possibleAttributes = objModel.getAttributes();
    // For search we aggregate the user metadata (x-amz-meta)
    // under the userMd attribute so add to possibilities
    possibleAttributes.userMd = true;
    const invalidAttribute = _validateTree(ast.where, possibleAttributes);
    if (invalidAttribute) {
        return errors.InvalidArgument.customizeDescription('Search param ' +
        `contains unknown attribute: ${invalidAttribute}`);
    }
    return undefined;
}

module.exports = validateSearchParams;
