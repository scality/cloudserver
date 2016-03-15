const baseName = 'ft-awsnodesdk-bucket';

const fakeDataSource = {
    generateBucketName() {
        const random = Math.round(Math.random() * 100).toString();

        return `${baseName}-${random}`;
    },

    generateManyBucketNames(numberOfBuckets) {
        const random = Math.round(Math.random() * 100).toString();

        return Array
            .from(Array(numberOfBuckets).keys())
            .map(i => `${baseName}-${random}-${i}`);
    },
};

export default fakeDataSource;
