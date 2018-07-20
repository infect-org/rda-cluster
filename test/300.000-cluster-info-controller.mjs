'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';



const host = 'http://l.dns.porn';



section('Cluster Info Controller', (section) => {
    let sm;
    let clusterDataSet = 'data-set-'+Math.round(Math.random()*1000000);
    let clusterDataSource = 'data-source-'+Math.round(Math.random()*1000000);


    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('rda-compute', 'rda-compute', 'rda-compute', 'rda-compute');
    });



    section.test('Create test cluster', async () => {
        const service = new Service();
        await service.load();

        const clusterResponse = await superagent.post(`${host}:${service.getPort()}/rda-cluster.cluster`).ok(res => res.status === 201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: clusterDataSet,
            dataSource: clusterDataSource,
        });
        
        assert(clusterResponse.body);
        assert(clusterResponse.body.clusterId);
        assert(clusterResponse.body.shards.length);

        await section.wait(200);
        await service.end();
    });



    section.test('Get cluster by identifier', async() => {
        const service = new Service();
        await service.load();

        const clusterResponse = await superagent.get(`${host}:${service.getPort()}/rda-cluster.cluster-info`).query({
            dataSource: clusterDataSource,
            dataSet: clusterDataSet,
        }).ok(res => res.status === 404).send();

        await section.wait(200);
        await service.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});