import { Controller } from 'rda-service';



/**
* returns information about a cluster
*/
export default class ClusterInfoController extends Controller {



    /**
    * @param      {Object}  arg1     options
    * @param      {<type>}  arg1.db  The database
     */
    constructor({
        db,
    }) {
        super('cluster-info');
        this.db = db;
        this.enableAction('list');
    }




    /**
    * returns information about a specific cluster
    *
    * @param      {Express.request}   request   express request
    * @param      {Express.response}   response  express response
    * @return     {Promise}  object cluster info
    */
    async list(request) {
        const { dataSource } = request.query();
        const dataSetIdentifier = request.query().dataSet;


        const cluster = await this.db.cluster('*', {
                dataSource,
                dataSetIdentifier,
            })
            .fetchClusterStatus('identifier', {
                identifier: 'active'
            })
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
                    url: shard.instance[0].url,
                };
            });

            // create a neat object that can be returned
            return {
                clusterId: cluster.id,
                clusterIdentifier: cluster.identifier,
                status: cluster.clusterStatus.identifier,
                shards,
                totalLoadedRecords,
            };
        } else request.response().status(404).send(`The cluster for the data source '${dataSource}' and the dataset '${dataSetIdentifier}' could not be found!`);
    }
}
