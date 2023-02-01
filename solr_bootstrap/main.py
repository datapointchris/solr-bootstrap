import json
from itertools import islice
from time import sleep

import requests


class Configuration:
    def __init__(self, config: dict):
        self.collection = config['collection']
        self.core_host = config['coreHost']
        self.num_partitions = config['numPartitions']
        self.concurrency = config['concurrency']
        self.partitions_per_batch = config['partitionsPerBatch']
        self.bootstrap_base_url = (
            f"http://{self.core_host}/searches/v1/rpc/{self.collection}/refreshAll/force"
        )
        self.bootstrap_healthcheck_url = (
            f"http://{self.core_host}/searches/v1/{self.collection}/bootstrap/healthcheck"
        )


def create_partition_indexes(num_partitions, partitions_per_batch):
    "Batch data into tuples of length n.  Take the tuple ends.  The last batch may be shorter."
    iterable = range(num_partitions)
    it = iter(iterable)
    while batch := tuple(islice(it, partitions_per_batch)):
        batch = (batch[0], batch[-1])
        yield batch


def create_partition_request_urls(config: Configuration, partition_indexes):
    partition_request_urls = []
    for index_start, index_end in partition_indexes:
        request_params = f"?numPartitions={config.num_partitions}&partitionIndexStart={index_start}&partitionIndexEnd={index_end}&concurrency={config.concurrency}"
        partition_request_urls.append(config.bootstrap_base_url + request_params)
    return partition_request_urls


def main():
    with open('vehicles.json', 'r') as f:
        config = json.load(f)

    conf = Configuration(config)

    partition_indexes = create_partition_indexes(conf.num_partitions, conf.partitions_per_batch)

    partition_request_urls = create_partition_request_urls(conf, partition_indexes)

    for request_url in partition_request_urls:
        # TODO: Add headers here
        response = requests.get(request_url)
        print(response.text)

        health = get_bootstrap_health_status(conf.bootstrap_healthcheck_url)
        



        while not all(health.statuses == 'COMP')


if __name__ == '__main__':
    main()
