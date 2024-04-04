from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

class OurReadWriteLock:
    def __init__(self, zk: KazooClient, path: str):
        self.path = path
        self.read_lock_path = f"{path}/read_lock_"
        self.write_lock_path = f"{path}/write_lock"
        self.read_locks = {}
        self.write_locks = {}
        self.zk = zk
        zk.ensure_path(path)

    def acquire_read(self) -> None:
        if self.zk.exists(self.write_lock_path):
            print("Write lock path already exists. Returning.")
            return
        
        znode_path = self.zk.create(self.read_lock_path, ephemeral=False, sequence=True)
        self.read_locks[znode_path] = True
        print(f"Read lock acquired: {znode_path} - znodes {self.get_children()}")

    def release_read(self) -> None:
        for znode_path in list(self.read_locks.keys()):
            self.zk.delete(znode_path)
            del self.read_locks[znode_path]
            print(f"Read lock released: {znode_path} - znodes {self.get_children()}")
        
        print(f"Znodes {self.get_children()}")
        
    def acquire_write(self) -> None:
        if self.zk.get_children(self.path):
            print("Can't acquire write lock because read locks or another write lock is present")
            return
        znode_path = self.zk.create(self.write_lock_path, ephemeral=False)
        self.write_locks[znode_path] = True
        print(f"Write lock acquired - znodes {self.get_children()}")
        
    def release_write(self) -> None:
        write_found = any(self.path + "/" + element == self.write_lock_path for element in self.get_children())
        print(write_found)
        if write_found:
            self.zk.delete(self.write_lock_path)
            print(f"Write lock released - znodes {self.get_children()}")

    def get_children(self) -> list:
        return self.zk.get_children(self.path)

    def remove_children(self) -> None:
        active_children = self.get_children()
        self.read_locks
        for znode_path in active_children:
            print(self.path + "/" + znode_path)
            self.zk.delete(self.path + "/" + znode_path)

        self.read_locks = {}
        self.write_locks = {}

