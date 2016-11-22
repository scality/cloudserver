function getType(v) {
    return Object.prototype.toString.call(v)
        .replace(/\[object\s(\w+)\]/, '$1').toLowerCase();
}

function validateAllFields(spec, candidate, prefix, errors, ctx) {
    const specType = getType(spec);
    const candidateType = getType(candidate);

    if (specType === 'regexp') {
        if (!spec.test(candidate)) {
            errors.push(`${prefix}: invalid string pattern`);
        }
        return;
    }

    if (specType === 'function') {
        if (!spec.call(ctx, candidate)) {
            errors.push(`${prefix}: failed predicate`);
        }
        return;
    }

    if (candidateType !== specType) {
        errors.push(`${prefix}: want ${specType}, have ${candidateType}`);
        return;
    }

    switch (specType) {
    case 'array':
        candidate.forEach((v, i) => {
            validateAllFields(spec[0], v, `${prefix}.${i}`, errors, ctx);
        });
        return;
    case 'object':
    case 'error':
        Object.keys(spec).forEach(f => {
            validateAllFields(spec[f], candidate[f],
                `${prefix}.${f}`, errors, ctx);
        });
        return;
    default:
        return;
    }
}

/**
 * Validate obj against spec.
 * @param {undefined|null|number|string|array|object|Error|RegExp} spec
 *      A value used by validateAllFields to test the properties of an object.
 *      If spec is a function (or contains function fields which will be
 *      passed recursively to validateAllFields), and the the _ctx argument
 *      is present, the function will be called in the context of the _ctx
 *      argument value. Custom spec functions are "predicates", and must
 *      return true or false;
 * @param {undefined|null|number|string|array|object|Error|RegExp} obj
 *      The candidate value to test against spec.
 * @param {array|object} _errs
 *      Optional. A user-supplied value to collect errors.
 *      Can be an array or any object with a `push` method.
 *      If _errs is not supplied, a local array will be created
 *      and returned.
 * @param {object} _ctx
 *      Optional. A user-supplied value to be used as `this` inside
 *      the spec function(s). If no _ctx argument is provided, a
 *      temporary local object will be used.
 * @return {array} user-supplied _errs object or new array.
 */
function validate(spec, obj, _errs, _ctx) {
    let errors;
    let ctx;
    if (_errs && typeof _errs.push === 'function') {
        errors = _errs;
    } else if (typeof _errs === 'object') {
        ctx = _errs;
    }

    if (!errors) {
        errors = [];
    }

    if (typeof _ctx === 'object') {
        ctx = _ctx;
    }

    validateAllFields(spec, obj, '$', errors, ctx);
    return errors;
}

export default validate;
