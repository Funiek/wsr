from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
import uuid
import time

class OurDoubleBarrier:
    def __init__(self, zk: KazooClient, path: str, how_many: int, session_id: str):
        self.path = path
        self.how_many = how_many
        self.session_id = session_id
        self.barrier_session_path = f"{path}/barrier_{session_id}"
        self.znode_path = None 
        self.zk = zk
        zk.ensure_path(path)

    def enter(self) -> None:
        print("Enter: Start")
        self.znode_path = self.zk.create(self.barrier_session_path, ephemeral=True)
        while True:
            children = self.get_children()
            if self.how_many <= len(children):
                print("Enter: Limit reached")
                break;

        print("Enter: End")

    def leave(self) -> None:
        print("Leave: Start")

        if self.zk.exists(self.znode_path):
            self.zk.delete(self.znode_path)

        while True:
            children = self.get_children()
            if 0 == len(children):
                print("Leave: All children left")
                break;

        print("Leave: End")

    def get_children(self) -> list:
        return self.zk.get_children(self.path)

    def remove_children(self) -> None:
        active_children = self.get_children()
        self.read_locks
        for znode_path in active_children:
            print(self.path + "/" + znode_path)
            self.zk.delete(self.path + "/" + znode_path)


