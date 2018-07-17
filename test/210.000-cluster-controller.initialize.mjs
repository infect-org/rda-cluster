'use strict';

import Service from '../index.mjs';
import section from 'section-tests';
import superagent from 'superagent';
import assert from 'assert';
import log from 'ee-log';
import {ServiceManager} from 'rda-service';
import {ShardedDataSet} from 'rda-fixtures';




const host = 'http://l.dns.porn';



section('Cluster Controller: Initialize', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry', 'infect-rda-sample-storage');
        await sm.startServices('rda-compute');


    });



    section.test('Initialize cluster', async () => {
        section.setTimeout(5000);

        const service = new Service();
        await service.load();


        section.notice('create cluster with one shard');
        const clusterResponse = await superagent.post(`${host}:${service.getPort()}/rda-cluster.cluster`).ok(res => res.status === 201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: 'data-set-'+Math.round(Math.random()*1000000),
            dataSource: 'data-source-'+Math.round(Math.random()*1000000),
        });


        section.notice('create data set & shard it');
        const dataSet = new ShardedDataSet();
        const shardName = await dataSet.create({
            name: clusterResponse.body.shards[0].identifier
        });


        section.notice('initialize cluster');
        await superagent.patch(`${host}:${service.getPort()}/rda-cluster.cluster/${clusterResponse.body.clusterId}`).ok(res => res.status === 200).send();



        section.notice('waiting for cluster initialization');
        while (true) {
            const res = await superagent.get(`${host}:${service.getPort()}/rda-cluster.cluster/${clusterResponse.body.clusterId}`).ok(res => [200, 201].includes(res.status)).send();
            if (res.status === 201) break;
        }
        
        await section.wait(200);
        await service.end();
    });




    section.destroy(async() => {
        await sm.stopServices();
    });
});