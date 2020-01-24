import assert from 'assert';
import HTTP2Client from '@distributed-systems/http2-client';
import log from 'ee-log';
import section from 'section-tests';
import Service from '../index.js';
import ServiceManager from '@infect/rda-service-manager';



const host = 'http://l.dns.porn';



section('Cluster Controller', (section) => {
    let sm;

    section.setup(async() => {
        sm = new ServiceManager({
            args: '--dev.testing --log-level=error+ --log-module=*'.split(' ')
        });

        await sm.startServices('@infect/rda-service-registry');
        await sm.startServices('@infect/rda-compute', '@infect/rda-compute', '@infect/rda-compute', '@infect/rda-compute');
    });



    section.test('Create cluster', async () => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        const clusterResponse = await client.post(`${host}:${service.getPort()}/rda-cluster.cluster`).expect(201).send({
            requiredMemory: 1000000,
            recordCount: 10000,
            dataSet: 'data-set-'+Math.round(Math.random()*1000000),
            dataSource: 'data-source-'+Math.round(Math.random()*1000000),
        });

        const data = await clusterResponse.getData();

        assert(data);
        assert(data.clusterId);
        assert(data.shards.length);

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.test('Get cluster by identifier, negative result', async() => {
        const service = new Service();
        const client = new HTTP2Client();
        await service.load();

        await client.get(`${host}:${service.getPort()}/rda-cluster.cluster/invalid`).expect(404).send();

        await section.wait(200);
        await service.end();
        await client.end();
    });



    section.destroy(async() => {
        await sm.stopServices();
    });
});