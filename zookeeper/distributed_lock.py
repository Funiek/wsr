from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
import time
import random

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

lock = Lock(zk, "/distributed_lock")

def do_with_lock():
    with lock:
        print("Lock acquired, critical section")
        time.sleep(random.randint(1, 5))
        print("Exiting critical section")

for _ in range(5):
    do_with_lock()

zk.stop()