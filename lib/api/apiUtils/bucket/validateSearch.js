const Parser = require('node-sql-parser').Parser;
const parser = new Parser();
const { errors } = require('arsenal');

function validateSearchParams(searchParams) {
    // searchParams should just be sql where clause
    // For metadata: message.userMd.`x-amz-meta-color`=\"blue\"
    // For tags: message.tags.`x-amz-meta-color`=\"blue\"
    // For any other attribute: message.value.`content-length`=5
    let ast;
    try {
        ast =
            parser.parse(`SELECT * FROM t WHERE ${searchParams}`);
    } catch (e) {
        if (e) {
            console.log('e!!', e);
            return errors.InvalidArgument
            .customizeDescription('Invalid sql where clause ' +
            'sent as search query');
        }
    }
    console.log("ast!!!", JSON.stringify(ast));
    // add method to bucket model to get all this._data object keys
    // then validate the ast where left stuff against that plus
    // userMd and tags?
    // if error with that validation, return error.
    return undefined;
}

module.exports = validateSearchParams;
