
/*
  This code is based on code from https://github.com/olehch/sqltomongo
  with the following license:

  The MIT License (MIT)

  Copyright (c) 2016 Oleh

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

/**
 * A helper object to map SQL-like naming to MongoDB query syntax
 */
const exprMapper = {
    '=': '$eq',
    '<>': '$ne',
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
    'LIKE': '$regex',
};

/*
 * Parses object with WHERE clause recursively
 * and generates MongoDB `find` query object
 */
function parseWhere(root) {
    const operator = Object.keys(root)[0];

    // extract leaf binary expressions
    if (operator === 'AND') {
        const e1 = parseWhere(root[operator][0]);
        const e2 = parseWhere(root[operator][1]);

        // eslint-disable-next-line
        return { '$and' : [ 
            e1,
            e2,
        ] };
    } else if (operator === 'OR') {
        const e1 = parseWhere(root[operator][0]);
        const e2 = parseWhere(root[operator][1]);

        // eslint-disable-next-line
        return  { '$or' : [ 
            e1,
            e2,
        ] };
    }
    const field = root[operator][0];
    const expr = exprMapper[operator];
    const obj = {};
    obj[`value.${field}`] = { [expr]: root[operator][1] };

    return obj;
}

module.exports = parseWhere;
