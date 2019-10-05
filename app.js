'use strict';

import logd from 'logd';
import ConsoleTransport from 'logd-console-transport';
import Service from './index.mjs';


// set up the module logger
const log = logd.module('rda-cluster');






// run the service
const service = new Service();


// load it
service.load().then(() => {
    log.success(`The RDA Cluster Service is listening on port ${service.server.port}`);
}).catch(console.log);

