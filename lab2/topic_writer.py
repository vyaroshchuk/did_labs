import hazelcast

hz_client = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')

ex_topic = hz_client.get_topic("ex_topic").blocking()

for i in range(1, 101):
    ex_topic.publish(i)

hz_client.shutdown()
