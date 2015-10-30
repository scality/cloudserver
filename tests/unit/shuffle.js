export default function shuffle(array) {
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
}
