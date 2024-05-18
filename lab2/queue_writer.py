import time

import hazelcast
import threading

client = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')

queue = client.get_queue("bqueue")


def produce():
    for i in range(1, 101):
        queue.offer(str(i))
        # time.sleep(0.5)


producer_thread = threading.Thread(target=produce)

producer_thread.start()

producer_thread.join()
print(queue.is_empty().result())
client.shutdown()
