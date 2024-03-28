from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
import time

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

barrier = Barrier(zk, "/single_barrier")

def remove_barrier():
    barrier.remove() 
    print("Barrier removed")

remove_barrier()

zk.stop()
