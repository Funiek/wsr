from kazoo.client import KazooClient
from our_read_write_lock import OurReadWriteLock

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

lock = OurReadWriteLock(zk, "/out_lock")


zk.stop()