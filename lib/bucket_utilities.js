export default {
    markerFilter(marker, array) {
        for (let i = 0; i < array.length; i++) {
            if (marker >= array[i]) {
                array.shift();
                i--;
            } else {
                break;
            }
        }
        return array;
    },

    prefixFilter(prefix, array) {
        for (let i = 0; i < array.length; i++) {
            if (array[i].indexOf(prefix) !== 0) {
                array.splice(i, 1);
                i--;
            }
        }
        return array;
    },

    findNextMarker(currentIndex, array, responseObject) {
        let delimitedKey;
        let delimiterIndex;
        // Iterate through remainder of keys array to
        // find next key that would NOT get rolled
        // up into an existing CommonPrefix
        for (let i = currentIndex; i < array.length; i++) {
            if (responseObject.Delimiter) {
                delimiterIndex = array[i].indexOf(responseObject.Delimiter);
                delimitedKey = array[i].slice(0, delimiterIndex + 1);
            }
            if (!responseObject.hasCommonPrefix(delimitedKey)) {
                return array[i];
            }
        }
        return null;
    },

    isKeyInContents(responseObject, key) {
        return responseObject.Contents.some(val => val.Key === key);
    }
};
