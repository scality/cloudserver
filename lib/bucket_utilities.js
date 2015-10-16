const bucketUtilities = {

    markerFilter(marker, array) {
        const length = array.length;
        for (let i = 0; i < length; i += 1) {
            if (marker >= array[i]) {
                array.shift();
                i -= 1;
            } else {
                break;
            }
        }
        return array;
    },

    prefixFilter(prefix, array) {
        for (let i = 0; i < array.length; i += 1) {
            if (array[i].indexOf(prefix) !== 0) {
                array.splice(i, 1);
                i -= 1;
            }
        }
        return array;
    },

    findNextMarker(currentIndex, array, responseObject) {
        let NextMarker;
        let delimitedKey;
        let currentKey;
        let delimiterIndex;
        const length = array.length;
        // Iterate through remainder of keys array to
        // find next key that would NOT get rolled
        // up into an existing CommonPrefix
        for (let i = currentIndex; i < length; i += 1) {
            currentKey = array[i];
            if (responseObject.Delimiter) {
                delimiterIndex =
                currentKey.indexOf(responseObject.Delimiter);
                delimitedKey = currentKey.slice(0, delimiterIndex + 1);
            }
            if (!responseObject.hasCommonPrefix(delimitedKey)) {
                NextMarker = currentKey;
                break;
            }
        }
        return NextMarker;
    },

    isKeyInContents(responseObject, key) {
        const length = responseObject.Contents.length;
        for (let i = 0; i < length; i += 1) {
            if (responseObject.Contents[i].Key === key) {
                return true;
            }
        }
        return false;
    }
};

export default bucketUtilities;
