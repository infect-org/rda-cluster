'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';



const host = 'http://l.dns.porn:8060';



section('Cluster Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
    });



    section.test('Get cluster by identifier, negative result', async() => {
        const service = new Service();
        await service.load();

        await superagent.get(`${host}:${service.getPort()}/rda-cluster.cluster/invalid`).ok(res => res.status === 404).send();

        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});