const { errors, models } = require('arsenal');
const { NotificationConfiguration } = models;

const { config } = require('../../../Config');

function getNotificationConfiguration(parsedXml) {
    const notifConfig = new NotificationConfiguration(parsedXml).getValidatedNotificationConfiguration();
    // if notifConfig is empty object, effectively delete notification configuration
    if (notifConfig.error || Object.keys(notifConfig).length === 0) {
        return notifConfig;
    }
    if (!config.bucketNotificationDestinations) {
        return { error: errors.InvalidArgument.customizeDescription(
            'Unable to validate the following destination configurations') };
    }
    const targets = new Set(config.bucketNotificationDestinations.map(t => t.resource));
    const notifConfigTargets = notifConfig.queueConfig.map(t => t.queueArn.split(':')[5]);
    // getting invalid targets
    const invalidTargets = [];
    notifConfigTargets.forEach((t, i) => {
        if (!targets.has(t)) {
            invalidTargets.push({
                ArgumentName: notifConfig.queueConfig[i].queueArn,
                ArgumentValue: 'The destination queue does not exist',
            });
        }
    });
    if (invalidTargets.length > 0) {
        const errDesc = 'Unable to validate the following destination configurations';
        let error = errors.InvalidArgument.customizeDescription(errDesc);
        error = error.addMetadataEntry('invalidArguments', invalidTargets);
        return { error };
    }
    return notifConfig;
}

module.exports = getNotificationConfiguration;
