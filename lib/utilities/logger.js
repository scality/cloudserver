import Logger from 'werelogs';
import bunyanLogstash from 'bunyan-logstash';

import Config from '../Config';

const _config = new Config();

export const logger = new Logger('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
    streams: [
        { stream: process.stdout },
        {
            type: 'raw',
            stream: bunyanLogstash.createStream({
                host: _config.log.logstash.host,
                port: _config.log.logstash.port,
            }),
        },
    ],
});
