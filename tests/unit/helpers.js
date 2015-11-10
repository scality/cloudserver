export default {
    makeid(size) {
        let text = '';
        const possible =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (let i = 0; i < size; i += 1) {
            text += possible
                .charAt(Math.floor(Math.random() * possible.length));
        }
        return text;
    },
    shuffle(array) {
        let randomIndex;
        let temporaryValue;
        const length = array.length;
        array.forEach((item, currentIndex, array) => {
            randomIndex = Math.floor(Math.random() * length);
            temporaryValue = array[currentIndex];
            array[currentIndex] = array[randomIndex];
            array[randomIndex] = temporaryValue;
        });
        return array;
    },
    timeDiff(startTime) {
        const timeArray = process.hrtime(startTime);
        // timeArray[0] is whole seconds
        // timeArray[1] is remaining nanoseconds
        const milliseconds = (timeArray[0] * 1000) + (timeArray[1] / 1e6);
        return milliseconds;
    }
};
