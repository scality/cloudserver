function setEnv(name, value) {
    if (value === undefined) {
        delete process.env[name];
    } else {
        process.env[name] = value;
    }
}

/**
 * Require a module uncached, applying temporary env vars for load-time.
 * Resets both env and require cache after loading.
 * @param {string} modulePath - absolute module path, use require.resolve
 * @param {object} env - env variables to set, an undefined value unsets it
 * @return {*} - exports of the freshly loaded module
 */
function requireWithEnv(modulePath, env) {
    const resolvedPath = require.resolve(modulePath); // ensure exact cache key
    const cachedModule = require.cache[resolvedPath];
    const previousEnv = {};
    Object.keys(env).forEach(name => {
        previousEnv[name] = process.env[name];
        setEnv(name, env[name]);
    });
    delete require.cache[resolvedPath];
    try {
        return require(resolvedPath);
    } finally {
        Object.keys(previousEnv).forEach(name => setEnv(name, previousEnv[name]));
        if (cachedModule) {
            require.cache[resolvedPath] = cachedModule;
        } else {
            delete require.cache[resolvedPath];
        }
    }
}

module.exports = requireWithEnv;
