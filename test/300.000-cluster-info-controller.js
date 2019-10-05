import Service from '../index.js';
import section from 'section-tests';
import HTTP2Client from '@distributed-systems/http2-client';
import assert from 'assert';
import log from 'ee-log';
import ServiceManager from '@infect/rda-service-manager';



const host = 'http://l.dns.porn';



section('Cluster Info Controller', (section) => {
    let sm;
    let clusterDataSet = 'data-set-'+Math.round(Math.random()*1000000);
    let clusterDataSource = 'data-source-'+Math.round(Math.random()*1000000);


    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });
        
        await sm.startServices('rda-service-registry');
        await sm.startServices('rda-compute', 'rda-compute', 'rda-compute', 'rda-compute');
    });



    section.test('Create test cluster', async () => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-cluster.cluster`).expect(201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: clusterDataSet,
            dataSource: clusterDataSource,
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

        const clusterResponse = await client.get(`${host}:${service.getPort()}/rda-cluster.cluster-info`).query({
            dataSource: clusterDataSource,
            dataSet: clusterDataSet,
        }).expect(404).send();

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});