import { Controller } from '@infect/rda-service';
import type from 'ee-types';
import logd from 'logd';
import l from 'ee-log';
import MaxDistributionLoadBalancer from '../load-balancers/MaxDistribution.js';
import NodeManager from '../NodeManager.js';
import HTTP2Client from '@distributed-systems/http2-client';


const log = logd.module('rda-cluster-controller');





export default class ClusterController extends Controller {


    /**
     * @param      {Object}            arg1                 options
     * @param      {Related.Database}  arg1.db              The database
     * @param      {RegistryClient}    arg1.registryClient  The registry client
     */
    constructor({
        db,
        registryClient,
    }) {
        super('cluster');

        this.registryClient = registryClient;
        this.db = db;


        // manages nodes and does some process wide locking
        // so that nodes don't get added to different clusters
        this.nodeManager = new NodeManager({ db });


        this.enableAction('listOne');
        this.enableAction('create');
        this.enableAction('update');


        this.httpClient = new HTTP2Client();
    }




    /**
     * shut down the controller
     */
    async end() {
        await this.httpClient.end();
    }




    /**
     * invalidate all clusters as long we're running on one host
     *
     * @return     {Promise}
     */
    async load() {
        await super.load();

        const status = await this.db.clusterStatus({
            identifier: 'ended',
        }).findOne();


        // invalidate all clusters
        await this.db.cluster().update({
            id_clusterStatus: status.id,
        });
    }






    /**
     * instructs a cluster to initialize, which means it should instruct its compute instances to
     * load their data. The status gets stored in the database.
     *
     * @param      {Express.request}   request   express request
     * @param      {Express.response}   response  express response
     * @return     {Promise}
     */
    async update(request) {
        const clusterId = request.getParameter('id');


        // make sure no other call can work on the cluster
        await this.nodeManager.lock(async() => {


            // check if the cluster exists and has a viable status
            // for initializing it
            const cluster = await this.db.cluster('*', {
                id: clusterId,
            }).fetchClusterStatus('identifier').raw().findOne();


            if (cluster) {
                if (cluster.clusterStatus.identifier === 'created') {

                    // get all nodes & shards for the cluster
                    const shards = await this.db.shard('*').fetchInstance('*').getCluster({
                        id: clusterId,
                    }).raw().find();


                    // call all nodes, tell them to initialize
                    await Promise.all(shards.map((shard) => {
                        const instance = shard.instance[0];

                        return this.httpClient.post(`${instance.url}/rda-compute.data-set`).expect(201).send({
                            dataSource: 'infect-rda-sample-storage',
                            shardIdentifier: shard.identifier,
                            minFreeMemory: 25,
                        });
                    }));


                    // start polling each instance for the 
                    // data loading status
                    this.monitorInstances(clusterId, shards);

                } else request.response().status(409).send(`Cluster with the id '${clusterId}' has a non viable status '${cluster.clusterStatus.identifier}'! Can onlny initialize clusters with the status 'created'!`); 
            } else request.response().status(404).send(`Cluster with the id '${clusterId}' not found!`); 
        });
    }







    /**
    * poll compute instances in order to determine the cluster status
    */
    monitorInstances(clusterId, shards) {

        Promise.all(shards.map(async (shard) => {
            const instance = shard.instance[0];
            const response = await this.httpClient.get(`${instance.url}/rda-compute.data-set`).send();
            const data = await response.getData();

            // set status if available
            if (data && data.recordCount) {
                await this.updateInstanceStatus(instance.id, data.recordCount);
            }

            // log problems
            if (response.status() !== 200 && response.status() !== 201) console.log(data);

            // 201 = the instance has finished loading
            return response.status(201);
        })).then(async (results) => {

            // if there is one false entry we're not done yet
            if (results.includes(false)) {

                // wait some time, ask again
                setTimeout(() => {
                    this.monitorInstances(clusterId, shards);
                }, 1000);
            } else {
                await this.updateClusterStatus(clusterId, 'active');
            }
        }).catch(async (err) => {
            console.log(err);
            await this.updateClusterStatus(clusterId, 'failed');
        }).catch(l);
    }





    /**
    * update instance status
    */
    async updateInstanceStatus(instanceId, loadedRecordCount) {
        await this.db.instance({
            id: instanceId
        }).update({
            loadedRecordCount
        });
    }






    /**
    * update a clusters status
    */
    async updateClusterStatus(clusterId, status) {
        const dbStatus = await this.db.clusterStatus('id', {
            identifier: status
        }).raw().findOne();

        await this.db.cluster({
            id: clusterId
        }).limit(1).update({
            id_clusterStatus: dbStatus.id
        });
    }








    /**
     * creates a cluster based on the available resources and the memory requirements of the data
     * set
     *
     * @param      {Express.request}   request   express request
     * @param      {Express.response}   response  express response
     * @return     {Promise}  undefined
     */
    async create(request) {
        const data = await request.getData();

        if (!data) {
            request.response().status(400).send('Missing request body!');
        } else if (!type.object(data)) {
            request.response().status(400).send('Request body must be a json object!');
        } else if (!type.number(data.requiredMemory)) {
            request.response().status(400).send('Missing parameter \'requiredMemory\' in request body!');
        } else if (!type.number(data.recordCount)) {
            request.response().status(400).send('Missing parameter \'recordCount\' in request body!');
        } else if (!type.string(data.dataSet)) {
            request.response().status(400).send('Missing parameter \'dataSet\' in request body!');
        } else if (!type.string(data.dataSource)) {
            request.response().status(400).send('Missing parameter \'dataSource\' in request body!');
        } else {
            
            // get a lock so that other have to wait until we
            // are ready
            await this.nodeManager.lock(async() => {

                // get instances
                const instances = await this.getAvailableComputeNodeInstances();


                // keep it simple for now, distribute the load to all 
                // available compute instances
                const loadBalancer = new MaxDistributionLoadBalancer();
                const shardConfig = await loadBalancer.getShards({
                    computeNodes: instances,
                    requiredMemory: data.requiredMemory,
                    recordCount: data.recordCount,
                });


                // store shard config
                const cluster = await this.nodeManager.createCluster({
                    dataSource: data.dataSource, 
                    dataSet: data.dataSet, 
                    shardConfig: shardConfig
                });

                request.response().status(201).send({
                    clusterId: cluster.id,
                    clusterIdentifier: cluster.identifier,
                    shards: shardConfig.map(config => config.shardId)
                });
            });
        }
    }







    /**
    * gets all available compute nodes. persist them
    * in the db if they are not already in the db
    */
    async getAvailableComputeNodeInstances() {
        
        // resolve the cluster service
        const registryHost = await this.registryClient.registryHost;

        // check the status on the cluster service
        const registryResponse = await this.httpClient.get(`${registryHost}/rda-service-registry.service-instance`).query({
            serviceType: 'rda-compute',
        }).expect(200).send();


        const data = await registryResponse.getData();

        // sync with db
        await this.nodeManager.syncInstances(data);

        // gets all instances that are not used by others
        return this.nodeManager.getAvailableInstances();
    }








    /**
    * returns information about a specific cluster
    */
    async listOne(request) {
        const clusterId = request.getParameter('id');
        const isIdentifier = /[^0-9]/i.test(clusterId);
        const filter = {};

        if (isIdentifier) filter.identifier = clusterId;
        else filter.id = clusterId;

        const cluster = await this.db.cluster('*', filter)
            .fetchClusterStatus('identifier')
            .getShard('*')
            .getInstance('*')
            .raw()
            .findOne();

        if (cluster) {
            let totalLoadedRecords = 0;
            const shards = cluster.shard.map((shard) => {
                totalLoadedRecords += shard.instance[0].loadedRecordCount;

                return {
                    identifier: shard.identifier,
                    instanceIdentifier: shard.instance[0].identifier,
                    loadedRecordCount: shard.instance[0].loadedRecordCount,
                };
            });
            

            // create a neat object that can be returned
            const data = {
                clusterId: cluster.id,
                clusterIdentifier: cluster.identifier,
                status: cluster.clusterStatus.identifier,
                shards: shards,
                totalLoadedRecords: totalLoadedRecords,
            };


            switch (cluster.clusterStatus.identifier) {
                case 'initialized':
                case 'created':
                    request.response().status(200).send(data);
                    break;

                case 'active':
                    request.response().status(201).send(data);
                    break;

                case 'ended':
                    request.response().status(404).send(data);
                    break;

                case 'failed':
                default:
                    request.response().status(500).send(data);
                    break;
            }
        } else request.response().status(404).send(`The cluster with the identifier '${clusterId}' could not be found!`);
    }
}
