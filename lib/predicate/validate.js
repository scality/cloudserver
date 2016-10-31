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
