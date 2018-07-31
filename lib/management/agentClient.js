const managementAgentMessageType = {
    /** Message that contains the loaded overlay */
    NEW_OVERLAY: 1,
};


function isManagementAgentUsed() {
    return process.env.MANAGEMENT_USE_AGENT === '1';
}

module.exports = {
    managementAgentMessageType,
    isManagementAgentUsed,
};
