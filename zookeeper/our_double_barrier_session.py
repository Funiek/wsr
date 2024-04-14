from kazoo.client import KazooClient
from our_double_barrier import OurDoubleBarrier
import uuid
import time
import random

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
session_id = uuid.uuid4();
print(f"Session_id: {session_id}")

barrier = OurDoubleBarrier(zk, "/our_barrier", 2, session_id.hex)


def enter_barrier():
    barrier.enter()

    print(f"{time.ctime()}: Entered barrier. Doing random work")
    time.sleep(random.randint(2, 5))
    print(f"{time.ctime()}: Exiting barrier")

    barrier.leave()

for _ in range(1):
    enter_barrier()
