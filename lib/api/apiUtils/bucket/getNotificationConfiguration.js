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
    if (!notifConfigTargets.every(t => targets.has(t))) {
        return { error: errors.MalformedXML.customizeDescription(
            'queue arn target is not included in Cloudserver bucket notification config'),
        };
    }
    return notifConfig;
}

module.exports = getNotificationConfiguration;
