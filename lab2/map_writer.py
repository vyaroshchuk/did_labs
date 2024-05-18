import hazelcast

hz_client = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')

ex_map = hz_client.get_map('ex_map').blocking()

for i in range(0, 1001):
    ex_map.put(i, i)

hz_client.shutdown()
