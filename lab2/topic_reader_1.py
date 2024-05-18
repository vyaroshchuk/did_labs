import time

import hazelcast

hz_client = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')

ex_topic = hz_client.get_topic("ex_topic").blocking()


def print_on_message(topic_message):
    print("Reader 1 got message", topic_message.message)


if __name__ == "__main__":
    try:
        ex_topic.add_listener(print_on_message)
        time.sleep(time.time())
    except KeyboardInterrupt:
        hz_client.shutdown()
