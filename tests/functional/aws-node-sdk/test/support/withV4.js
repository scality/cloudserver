const versions = ['default', 'v4'];

function withV4(testFn) {
    versions.forEach(version => {
        let config;

        if (version === 'v4') {
            config = {
                signatureVersion: version,
            };
        } else {
            config = {};
        }

        describe(`With ${version} signature`, (cfg =>
            function tcWrap() {
                testFn.call(this, cfg);
            }
        )(config));
    });
}

export default withV4;
