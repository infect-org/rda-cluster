import Service from '../index.js';
import section from 'section-tests';
import HTTP2Client from '@distributed-systems/http2-client';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';
import { ShardedDataSet } from '@infect/rda-fixtures';



const host = 'http://l.dns.porn';



section('Cluster Info Controller', (section) => {
    let sm;
    let dataSet;


    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });

        await sm.startServices('@infect/rda-service-registry', '@infect/infect-rda-sample-storage');
        await sm.startServices('@infect/rda-compute-service', '@infect/rda-compute-service', '@infect/rda-compute-service', '@infect/rda-compute-service');

        dataSet = new ShardedDataSet();
        await dataSet.create();
    });



    section.test('Create test cluster', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-cluster.cluster`).expect(201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: dataSet.dataSetId,
            dataSource: dataSet.storageServiceName,
            modelPrefix: 'Infect',
        });

        const data = await clusterResponse.getData();

        assert(data);
        assert(data.clusterId);
        assert(data.shards.length);

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.test('Get cluster by identifier', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        await client.get(`${host}:${service.getPort()}/rda-cluster.cluster-info`).query({
            dataSource: dataSet.storageServiceName,
            dataSet: dataSet.dataSetId,
        }).expect(404).send();

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});