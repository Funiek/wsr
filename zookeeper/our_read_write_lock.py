from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
import time
import uuid

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

class OurReadWriteLock:
    def __init__(self, path):
        self.path = path
        self.read_lock_path = f"{path}/read_lock_"
        self.write_lock_path = f"{path}/write_lock"
        self.read_locks = {}
        self.write_locks = {}
        zk.ensure_path(path)

    def acquire_read(self):
        if zk.exists(self.write_lock_path):
            print("Write lock path already exists. Returning.")
            return
        
        znode_path = zk.create(self.read_lock_path, ephemeral=True, sequence=True)
        self.read_locks[znode_path] = True
        print(f"Read lock acquired: {znode_path}")

    def release_read(self):
        for znode_path in list(self.read_locks.keys()):
            zk.delete(znode_path)
            del self.read_locks[znode_path]
            print(f"Read lock released: {znode_path}")
        