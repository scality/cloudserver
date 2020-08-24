const { errors, models } = require('arsenal');
const { NotificationConfiguration } = models;

const { config } = require('../../../Config');

function getNotificationConfiguration(parsedXml) {
    const targets = [];
    if (!config.bucketNotificationDestinations) {
        return { error: errors.InvalidArgument.customizeDescription(
            'Unable to validate the following destination configurations') };
    }
    config.bucketNotificationDestinations.forEach(t => {
        targets.push(t.resource);
    });
    const notifConfig = new NotificationConfiguration(parsedXml).getValidatedNotificationConfiguration();
    if (notifConfig.error) {
        return notifConfig;
    }
    const notifConfigTargets = [];
    notifConfig.queueConfig.forEach(t => {
        const arnArray = t.queueArn.split(':');
        notifConfigTargets.push(arnArray[5]);
    });
    notifConfigTargets.forEach(t => {
        if (!targets.includes(t)) {
            notifConfig.error = errors.MalformedXML.customizeDescription(
                'queue arn target is not included in Cloudserver bucket notification config');
        }
    });
    return notifConfig;
}

module.exports = getNotificationConfiguration;
