'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import logd from 'logd';
import l from 'ee-log';
import superagent from 'superagent';


const log = logd.module('rda-cluster-controller');



export default class ClusterInfoController extends Controller {


    constructor({
        db,
    }) {
        super('cluster-info');
        this.db = db;
        this.enableAction('list');
    }




    /**
    * returns information about a specific cluster
    */
    async list(request, response) {
        const dataSource = request.query.dataSource;
        const dataSetIdentifier = request.query.dataSet;

        const cluster = await this.db.cluster('*', {
            dataSource,
            dataSetIdentifier,
        }).fetchClusterStatus('identifier', {
            identifier: 'active'
        }).getShard('*').getInstance('*').raw().findOne();


        if (cluster) {
            let totalLoadedRecords = 0;
            const shards = cluster.shard.map((shard) => {
                totalLoadedRecords += shard.instance[0].loadedRecordCount;

                return {
                    identifier: shard.identifier,
                    instanceIdentifier: shard.instance[0].identifier,
                    loadedRecordCount: shard.instance[0].loadedRecordCount,
                    url: shard.instance[0].url,
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