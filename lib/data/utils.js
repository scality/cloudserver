/**
 * proxyCompareURL - compares request endpoint to urls in NO_PROXY env var
 * @param {string} endpoint - url of request
 * @return {bool} true if request endpoint matches no proxy, false if not
 */
function proxyCompareURL(endpoint) {
    const noProxy = process.env.NO_PROXY || process.env.no_proxy;
    if (!noProxy) {
        return false;
    }
    // noProxy env var is a comma separated list of urls not to proxy
    const noProxyList = noProxy.split(',');
    if (noProxyList.includes(endpoint)) {
        return true;
    }
    const epArr = endpoint.split('.');
    // reverse array to make comparison easier
    epArr.reverse();
    let match = false;
    for (let j = 0; j < noProxyList.length; j++) {
        const urlArr = noProxyList[j].split('.');
        urlArr.reverse();
        for (let i = 0; i < epArr.length; i++) {
            if (epArr[i] === urlArr[i]) {
                match = true;
            } else if (urlArr[i] === '*' && i === (urlArr.length - 1)) {
                // if first character of url is '*', remaining endpoint matches
                match = true;
                break;
            } else if (urlArr[i] === '' && i === (urlArr.length - 1)) {
                // if first character of url is '.', it is treated as wildcard
                match = true;
                break;
            } else if (urlArr[i] === '*') {
                match = true;
            } else if (epArr[i] !== urlArr[i]) {
                match = false;
                break;
            }
        }
        // if endpoint matches noProxy element, stop checking
        if (match) {
            break;
        }
    }
    // if endpoint matches, request should not be proxied
    if (match) {
        return true;
    }
    return false;
}

module.exports = proxyCompareURL;
