from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
import time

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

barrier = Barrier(zk, "/single_barrier")

def wait_at_barrier():
    print(f"{time.ctime()}: Waiting at barrier")
    barrier.create() 
    barrier.wait() 
    print(f"{time.ctime()}: Crossing the barrier")

wait_at_barrier()

zk.stop()