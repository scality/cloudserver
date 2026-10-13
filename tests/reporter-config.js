// Name the report after the yarn script, so that runs sharing a directory
// (or copied to a single artifacts folder) do not overwrite each other.
const suite = process.env.npm_lifecycle_event || 'mocha';

module.exports = {
    reporterEnabled: 'spec, xunit',
    xunitReporterOptions: {
        output: `junit/junit-${suite}.xml`,
        suiteName: suite,
    },
};
