'use strict';


import RDAService from 'rda-service';
import path from 'path';
import logd from 'logd';
import Related from 'related';
import RelatedTimestamps from 'related-timestamps';

const log = logd.module('rda-cluster');



// controllers
import ClusterController from './controller/Cluster';





export default class ClusterService extends RDAService {


    constructor() {
        super('rda-cluster');
    }




    /**
    * prepare the service
    */
    async load() {

        // load database
        this.related = new Related(this.config.db);
        this.related.use(new RelatedTimestamps());

        await this.related.load();
        this.db = this.related[this.config.db.schema];

        const options = {
            db: this.db,
            registryClient: this.registryClient,
        };


        // register controllers
        this.registerController(new ClusterController(options));


        await super.load();


        // tell the service registry where we are
        await this.registerService();
    }





    /**
    * shut down the service
    */
    async end() {
        await super.end();
        await this.related.end();
    }
}