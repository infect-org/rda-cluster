import RDAService from '@infect/rda-service';
import path from 'path';
import logd from 'logd';
import Related from 'related';
import RelatedTimestamps from 'related-timestamps';

import ClusterController from './controller/ClusterController.js';
import ClusterInfoController from './controller/ClusterInfoController.js';


const log = logd.module('rda-cluster');



const appRoot = path.join(path.dirname(new URL(import.meta.url).pathname), '../');




export default class ClusterService extends RDAService {


    constructor() {
        super({
            name: 'rda-cluster',
            appRoot,
        });
    }




    /**
    * prepare the service
    */
    async load() {
        await this.initialize();

        // load database
        this.related = new Related(this.config.get('database'));
        this.related.use(new RelatedTimestamps());

        await this.related.load();
        this.db = this.related[this.config.get('database').schema];

        const options = {
            db: this.db,
            registryClient: this.registryClient,
        };


        // register controllers
        this.registerController(new ClusterController(options));
        this.registerController(new ClusterInfoController(options));


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
