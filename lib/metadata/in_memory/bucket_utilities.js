import naturalCompare from 'natural-compare-lite';

export function markerFilter(marker, array) {
    for (let i = 0; i < array.length; i++) {
        // If the marker is equal to or after an element
        // in the array, eliminate it.
        const laterItem = [marker, array[i]].sort(naturalCompare)[1];
        if (marker === array[i] || marker === laterItem) {
            array.shift();
            i--;
        } else {
            break;
        }
    }
    return array;
}

export function markerFilterMPU(allMarkers, array) {
    const { keyMarker, uploadIdMarker } = allMarkers;
    for (let i = 0; i < array.length; i++) {
        // If the keyMarker is the same as the key,
        // check the uploadIdMarker.  If uploadIdMarker is the same
        // as or alphabetically after the uploadId of the item,
        // eliminate the item.
        if (uploadIdMarker && keyMarker === array[i].key) {
            const laterId =
                [uploadIdMarker, array[i].uploadId].sort(naturalCompare)[1];
            if (array[i].uploadId === laterId) {
                break;
            } else {
                array.shift();
                i--;
            }
        } else {
        // If the keyMarker is alphabetically after the key
        // of the item in the array, eliminate the item from the array.
            const laterItem =
                [keyMarker, array[i].key].sort(naturalCompare)[1];
            if (keyMarker === array[i].key || keyMarker === laterItem) {
                array.shift();
                i--;
            } else {
                break;
            }
        }
    }
    return array;
}

export function prefixFilter(prefix, array) {
    for (let i = 0; i < array.length; i++) {
        if (array[i].indexOf(prefix) !== 0) {
            array.splice(i, 1);
            i--;
        }
    }
    return array;
}

export function findNextMarker(currentIndex, array, responseObject) {
    let delimiterIndex;
    let delimitedKey;
    // Iterate through remainder of array to
    // find next key that would NOT get rolled
    // up into an existing CommonPrefix
    for (let i = currentIndex; i < array.length; i++) {
        // If this function is called for a bucket listing, the key
        // will be array[i].
        // If this is for a multipart
        // upload listing, the key will be array[i].key.
        const key = array[i].key ? array[i].key : array[i];
        if (responseObject.Delimiter) {
            delimiterIndex = key.indexOf(responseObject.Delimiter);
            delimitedKey = key.slice(0, delimiterIndex + 1);
        }
        if (!responseObject.hasCommonPrefix(delimitedKey)) {
            // If this function is called for a bucket listing,
            // just return the key.  If this is for a multipart
            // upload listing, return an array containing the
            // key and the uploadId.
            const result = array[i].key ? [key, array[i].uploadId] : key;
            return result;
        }
    }
    return null;
}

export function isKeyInContents(responseObject, key) {
    return responseObject.Contents.some(val => val.key === key);
}
