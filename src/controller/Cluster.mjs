'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import logd from 'logd';
import l from 'ee-log';
import superagent from 'superagent';
import MaxDistributionLoadBalancer from '../load-balancers/MaxDistribution';
import NodeManager from '../NodeManager'


const log = logd.module('rda-cluster-controller');



export default class ClusterController extends Controller {


    constructor({
        db,
        registryClient,
    }) {
        super('cluster');

        this.registryClient = registryClient;
        this.db = db;




        // manages nodes and does some process wide locking
        // so that nodes don't get added to different clusters
        this.nodeManager = new NodeManager({db});


        this.enableAction('listOne');
        this.enableAction('create');
        this.enableAction('update');
    }




    /**
    * invalidate all clusters as long we're running on one host
    */
    async load() {
        await super.load();

        const status = await this.db.clusterStatus({
            identifier: 'ended'
        }).findOne();


        // invalidate all clusters
        await this.db.cluster().update({
            id_clusterStatus: status.id
        });
    }






    /**
    * instructs a cluster to initialize, which means it should
    * instruct its compute instances to load their data. The 
    * status gets stored in the database.
    */
    async update(request, response) {
        const clusterId = request.params.id;


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
                    await Promise.all(shards.map((shard => {
                        const instance = shard.instance[0];

                        return superagent.post(`${instance.url}/rda-compute.data-set`).ok(res => res.status === 201).send({
                            dataSource: 'infect-rda-sample-storage',
                            shardIdentifier: shard.identifier,
                            minFreeMemory: 25,
                        });
                    })));


                    // start polling each instance for the 
                    // data loading status
                    this.monitorInstances(clusterId, shards);

                } else response.status(409).send(`Cluster with the id '${clusterId}' has a non viable status '${cluster.clusterStatus.identifier}'! Can onlny initialize clusters with the status 'created'!`); 
            } else response.status(404).send(`Cluster with the id '${clusterId}' not found!`); 
        });
    }







    /**
    * poll compute instances in order to determine the cluster status
    */
    monitorInstances(clusterId, shards) {

        Promise.all(shards.map(async (shard) => {
            const instance = shard.instance[0];
            const response = await superagent.get(`${instance.url}/rda-compute.data-set`).ok(r => true).send();

            // set status if available
            if (response.body && response.body.recordCount) {
                await this.updateInstanceStatus(instance.id, response.body.recordCount);
            }

            // log problems
            if (response.status !== 200 && response.status !== 201) console.log(response.body);

            // 201 = the instance has finished loading
            return response.status === 201;
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
    * creates a cluster base don the available resources
    * and the memory requirements of the data set
    */
    async create(request, response) {
        const data = request.body;

        if (!data) response.status(400).send(`Missing request body!`);
        else if (!type.object(data)) response.status(400).send(`Request body must be a json object!`);
        else if (!type.number(data.requiredMemory)) response.status(400).send(`Missing parameter 'requiredMemory' in request body!`);
        else if (!type.number(data.recordCount)) response.status(400).send(`Missing parameter 'recordCount' in request body!`);
        else if (!type.string(data.dataSet)) response.status(400).send(`Missing parameter 'dataSet' in request body!`);
        else if (!type.string(data.dataSource)) response.status(400).send(`Missing parameter 'dataSource' in request body!`);
        else {
            
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

                response.status(201).send({
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
        const registryResponse = await superagent.get(`${registryHost}/rda-service-registry.service-instance`).query({
            serviceType: 'rda-compute',
        }).ok(res => res.status === 200).send();

        // sync with db
        await this.nodeManager.syncInstances(registryResponse.body);

        // gets all instances that are not used by others
        return this.nodeManager.getAvailableInstances();
    }








    /**
    * returns information about a specific cluster
    */
    async listOne(request, response) {
        const clusterId = request.params.id;
        const isIdentifier = /[^0-9]/i.test(clusterId);
        const filter = {};

        if (isIdentifier) filter.identifier = clusterId;
        else filter.id = clusterId;

        const cluster = await this.db.cluster('*', filter).fetchClusterStatus('identifier').getShard('*').getInstance('*').raw().findOne();


        if (cluster) {
            let totalLoadedRecords = 0;
            const shards = cluster.shard.map((shard) => {
                totalLoadedRecords += shard.instance[0].loadedRecordCount;

                return {
                    identifier: shard.identifier,
                    instanceIdentifier: shard.instance[0].identifier,
                    loadedRecordCount: shard.instance[0].loadedRecordCount,
                };
            })

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
                    response.status(200).send(data);
                    break;

                case 'active':
                    response.status(201).send(data);
                    break;

                case 'ended':
                    response.status(404).send(data);
                    break;

                case 'failed':
                default:
                    response.status(500).send(data);
                    break;
            }
        } else response.status(404).send(`The cluster with the identifier '${clusterId}' could not be found!`);
    }
}