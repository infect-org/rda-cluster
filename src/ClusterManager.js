


/**
 * the cluster manager is responsible for creating and changing clusters, it is kept consistent
 * using the lock service which ensures that all actions executed on the cluster are atomic. this
 * class is notified when a cluster node goes down so that it can fix or restart the cluster.
 */
export default class ClusterManager {


    constructor() {
        this.a = 1;
    }




    /**
     * facotry function: acquire a lock for a given resource with a globally unique identifier
     *
     * @param      {string}   identifier  resource identifier
     * @param      {number}   ttl         ttl for the lock in seconds. the lock will be freed
     *                                    automatically after the ttl is run out
     * @param      {number}   timeout     how long should we wait for acquiring the lock in seconds?
     * @return     {Promise}  The lock instance representing the lock
     */
    async lock(identifier, ttl, timeout) {

    }
}