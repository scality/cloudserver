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

export function isKeyInContents(responseObject, key) {
    return responseObject.Contents.some(val => val.key === key);
}
