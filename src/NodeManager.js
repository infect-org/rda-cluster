import EventEmitter from 'events';
import uuid from 'uuid';



/**
* PAY ATTENATION: this code can only work if exactly one service instance
* of this service is active! Please implement locks if you wish to run
* multiple instances!
* The code below only regulates concurrent access per class instance!
*/





export default class NodeInstanceManager extends EventEmitter {


    constructor({
        db,
    }) {
        super();

        this.db = db;

        // make sure that nothing is executed in parallel 
        // since that would cause mayhem
        this.queues = new Map();
        this.executionStatus = new Map();
    }






    /**
    * create a cluster with the given shard config
    */
    async _createCluster({
        dataSource,
        dataSet,
        shardConfig,
    }) {
        const cluster = await new this.db.cluster({
            identifier: uuid.v4(),
            dataSetIdentifier: dataSet,
            dataSource,
            shard: shardConfig.map((config) => {
                return new this.db.shard({
                    identifier: config.shardId,
                    instance: this.db.instance({
                        identifier: config.instanceId,
                    }),
                });
            }),
            clusterStatus: this.db.clusterStatus({
                identifier: 'created',
            }),
        }).save();

        return cluster.toJSON();
    }





    /**
    * create a cluster with the given shard config
    */
    createCluster(dataSet, shardConfig) {
        return this.enqueue({
            method: '_createCluster',
            args: [dataSet, shardConfig],
        });
    }







    /**
    * returns a lock that can be freed. the lock is used
    * for the time between the available instances are
    * queried and the time the instances are assigned to
    * a shard and cluster.
    */
    lock(fn) {
        return this.enqueue({
            method: '_lock',
            args: [fn],
        });
    }


    async _lock(fn) {
        return fn();
    }





    /**
    * returns all instances that are currently
    * available in the db
    */
    async getAvailableInstances() {
        return this.db.instance('*', {
            id_shard: null,
        }).raw().find();
    }






    /**
    * make sure that all compute-node instances
    * are in the database. remove all that are
    * not available anymore
    */
    async _syncInstances(instances) {
        const liveInstances = new Map(instances.map(instance => [instance.identifier, instance]));
        const existingInstances = await this.db.instance('identifier').raw().find();
        const dbInstances = new Set(existingInstances.map(instance => instance.identifier));


        // remove non live instances
        for (const dbInstance of existingInstances) {
            if (!liveInstances.has(dbInstance.identifier)) {
                this.emit('instance_removed', dbInstance);

                await this.db.instance({
                    identifier: dbInstance.identifier
                }).limit(1).delete();
            }
        }


        // add new ones
        for (const instance of liveInstances.values()) {
            if (!dbInstances.has(instance.identifier)) {


                // make sure the node is in the db
                await this.ensureNode(instance.machineId);

                // add to db
                const dbInstance = await new this.db.instance({
                    identifier: instance.identifier,
                    memory: instance.availableMemory,
                    url: instance.ipv4address,
                    node: this.db.node({
                        identifier: instance.machineId
                    }),
                }).save();

                this.emit('instance_added', dbInstance.toJSON());
            }
        }
    }





    /**
    * makes sure a node has a representation in the db
    */
    async ensureNode(nodeIdentifier) {
        const node = await this.db.node({
            identifier: nodeIdentifier
        }).findOne();

        if (!node) {
            await new this.db.node({
                identifier: nodeIdentifier
            }).save();
        }
    }





    /**
    * make sure that all compute-node instances 
    * are in the database. remove all that are 
    * not available anymore
    */
    syncInstances(instances) {
        return this.enqueue({
            method: '_syncInstances',
            args: [instances]
        });
    }




    /**
    * execute the next item from the queue
    */
    executeQueue(method) {
        if (this.queues.has(method) && this.queues.get(method).length) {
            if (!this.executionStatus.get(method)) {
                this.executionStatus.set(method, true);

                const {args, resolve, reject} = this.queues.get(method).shift();

                this[method](...args).then((result) => {
                    this.executionStatus.set(method, false);
                    resolve(result);
                    this.executeQueue(method);
                }).catch((err) => {
                    this.executionStatus.set(method, false);
                    reject(err);
                    this.executeQueue(method);
                });
            }
        }
    }




    /**
    * add items to the execution queue
    */
    enqueue({
        method,
        args,
    }) {
        if (!this.queues.has(method)) this.queues.set(method, []);

        const promise = new Promise((resolve, reject) => {
            this.queues.get(method).push({
                resolve,
                reject,
                args,
            });
        });


        // return, then execute the item
        setImmediate(() => {
            this.executeQueue(method);
        });


        return promise;
    }
}
