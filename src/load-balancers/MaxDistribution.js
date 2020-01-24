import uuid from 'uuid';


export default class MaxDistibutionLoadBalancer {



    /**
    * return the shard configuration, that is a mapping
    * between instances and shards
    */
    async getShards({
        computeNodes,
    }) {
        return computeNodes.map((node) => ({
            shardId: uuid.v4(),
            instanceId: node.identifier,
        }));
    }
}