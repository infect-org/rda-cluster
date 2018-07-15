'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import logd from 'logd';
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
                const cluster = await this.nodeManager.createCluster(data.dataSet, shardConfig);

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


        const cluster = await this.db.cluster({
            identifier: clusterId
        }).findOne();


        if (cluster) {
            throw new Error(`Not implemented`);
        } else response.status(404).send(`he cluster with the identifier '${clusterId}' could not be found!`);
    }
}