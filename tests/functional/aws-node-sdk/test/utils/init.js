const metadata = require('../../../../../lib/metadata/wrapper');
let metadataInit = false;

function initMetadata(done) {
	if (metadataInit === true) {
		return done();
	}
	return metadata.setup(err => {
		if (err) {
			return done(err);
		}
		metadataInit = true;
		return done();
	});
}

module.exports = {
	initMetadata,
};
