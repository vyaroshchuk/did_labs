import hazelcast
import threading

client = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')

queue = client.get_queue("bqueue")


def consume():
    while True:
        head = queue.take().result()
        print("Reader 1: {}".format(head))


consumer_thread = threading.Thread(target=consume)

consumer_thread.start()

consumer_thread.join()

client.shutdown()
