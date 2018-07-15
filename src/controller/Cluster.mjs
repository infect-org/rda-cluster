'use strict';


import {Controller} from 'rda-service';
import type from 'ee-types';
import logd from 'logd';


const log = logd.module('rda-cluster-controller');



export default class ClusterController extends Controller {


    constructor({
        db,
        registryClient,
    }) {
        super('cluster');

        this.registryClient = registryClient;
        this.db = db;

        this.enableAction('listOne');
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