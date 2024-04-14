from kazoo.client import KazooClient
from our_read_write_lock import OurReadWriteLock
import uuid
import time
import random

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
session_id = uuid.uuid4();

lock = OurReadWriteLock(zk, "/our_lock")

print(f"Session_id: {session_id}")

def acquire_lock():
    if random.randint(1, 5) % 2:
        print(f"{time.ctime()}: Trying to acquire read lock")
        lock.acquire_read()
        time.sleep(random.randint(1, 5))
        lock.release_read()
        print(f"{time.ctime()}: Exiting lock")
    else:
        print(f"{time.ctime()}: Trying to acquire write lock")
        lock.acquire_write()
        time.sleep(random.randint(1, 5))
        lock.release_write()
        print(f"{time.ctime()}: Exiting lock")

for _ in range(5):
    acquire_lock()
        
zk.stop()