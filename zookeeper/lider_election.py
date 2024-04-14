from kazoo.client import KazooClient
from kazoo.recipe.election import Election
import time
import uuid

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
session_id = uuid.uuid4();


def leader_callback():
    print(f"{time.ctime()}: Session {str(session_id)}: Became the leader")
    time.sleep(5)  
    print(f"{time.ctime()}: Session {str(session_id)}: Ending leadership")

election = Election(zk, "/leader_election")
election.run(leader_callback)

zk.stop()