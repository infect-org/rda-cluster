import assert from 'assert';
import HTTP2Client from '@distributed-systems/http2-client';
import log from 'ee-log';
import section from 'section-tests';
import Service from '../index.js';
import ServiceManager from '@infect/rda-service-manager';
import { ShardedDataSet } from '@infect/rda-fixtures';




const host = 'http://l.dns.porn';



section('Cluster Controller: Initialize', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('@infect/rda-service-registry', '@infect/infect-rda-sample-storage');
        await sm.startServices('@infect/rda-compute-service');
    });



    section.test('Initialize cluster', async () => {
        section.setTimeout(5000);

        const service = new Service();
        const client = new HTTP2Client();
        await service.load();


        section.notice('create data set & shard it');
        const dataSet = new ShardedDataSet();
        await dataSet.create();


        section.notice('create cluster with one shard');
        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-cluster.cluster`).expect(201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: dataSet.dataSetId,
            dataSource: dataSet.storageServiceName,
            modelPrefix: 'Infect',
        });

        const clusterResponseData = await clusterResponse.getData();



        section.notice('initialize cluster');
        await client.patch(`${host}:${service.getPort()}/rda-cluster.cluster/${clusterResponseData.clusterId}`).expect(200).send();



        section.notice('waiting for cluster initialization');
        while (true) {
            const res = await client.get(`${host}:${service.getPort()}/rda-cluster.cluster/${clusterResponseData.clusterId}`).expect(200, 201).send();
            if (res.status(201)) break;
            section.wait(100);
        }

        await section.wait(200);
        await service.end();
        await client.end();
    });




    section.destroy(async() => {
        await sm.stopServices();
    });
});