import time
import socket
import pickle
import log
from message import Message
from role import Acceptor, Leader

LOG = log.LOG

if __name__ == '__main__':
    numclients = 5
    LOG.set_proposer(2)
    LOG.set_acceptor(numclients)
    clients = [Acceptor(port, port - 61000, [60000, 60001]) for port in xrange(61000, 61000 + numclients)]
    leader = Leader(60000, 0, [60001], [c.port for c in clients])
    leader2 = Leader(60001, 1, [60000], [c.port for c in clients])
    leader.start()
    leader2.start()
    for c in clients:
        c.start()

    clients[0].fail()
    clients[1].fail()

    # Send some proposals through to test
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start = time.time()

    m = Message(Message.MSG_EXT_PROPOSE)
    m.instanceID = 1
    m.value = 1
    m.to = 60001
    bytes = pickle.dumps(m)
    print '# ATTEMPT 1'
    s.sendto(bytes, ('localhost', m.to))

    time.sleep(2)
    m.instanceID = 1
    m.value = 2
    m.to = 60000
    bytes = pickle.dumps(m)
    print '# ATTEMPT 2'
    s.sendto(bytes, ('localhost', m.to))

    time.sleep(2)
    m.instanceID = 1
    m.value = 3
    m.to = 60000
    bytes = pickle.dumps(m)
    print '# ATTEMPT 3'
    s.sendto(bytes, ('localhost', m.to))

    end = time.time()

    print "Sleeping for 10s"
    time.sleep(10)
    print "Stopping leaders"
    leader.stop()
    leader2.stop()
    print "Stopping clients"
    for c in clients:
        c.stop()

    print "Leader 1 history: ", leader.getHistory()
    print "Leader 2 history: ", leader2.getHistory()
    LOG.draw_results(print_all=True)
