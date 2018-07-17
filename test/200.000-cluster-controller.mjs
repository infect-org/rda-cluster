'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';



const host = 'http://l.dns.porn';



section('Cluster Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('rda-compute', 'rda-compute', 'rda-compute', 'rda-compute');
    });



    section.test('Create cluster', async () => {
        const service = new Service();
        await service.load();

        const clusterResponse = await superagent.post(`${host}:${service.getPort()}/rda-cluster.cluster`).ok(res => res.status === 201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: 'data-set-'+Math.round(Math.random()*1000000),
            dataSource: 'data-source-'+Math.round(Math.random()*1000000),
        });
        
        assert(clusterResponse.body);
        assert(clusterResponse.body.clusterId);
        assert(clusterResponse.body.shards.length);

        await section.wait(200);
        await service.end();
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