import time
import socket
import pickle
import log
from message import Message
from role import Acceptor, Proposer

LOG = log.LOG

if __name__ == '__main__':
    numclients = 5
    LOG.set_proposer(2)
    LOG.set_acceptor(numclients)
    clients = [Acceptor(port, port - 61000, [60000, 60001]) for port in xrange(61000, 61000 + numclients)]
    proposer = Proposer(60000, 0, [60001], [c.port for c in clients])
    proposer2 = Proposer(60001, 1, [60000], [c.port for c in clients])
    proposer.start()
    proposer.setPrimary(True)
    proposer2.setPrimary(True)
    proposer2.start()
    for c in clients:
        c.start()

    clients[0].fail()
    clients[1].fail()

    # Send some proposals through to test
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start = time.time()

    m = Message(Message.MSG_EXT_PROPOSE)
    m.instanceID = 1
    m.value = 100
    m.to = 60001
    bytes = pickle.dumps(m)
    print '\n# ATTEMPT 1'
    s.sendto(bytes, ('localhost', m.to))

    time.sleep(2)
    m.instanceID = 1
    m.value = 2
    m.to = 60000
    bytes = pickle.dumps(m)
    print '\n# ATTEMPT 2'
    s.sendto(bytes, ('localhost', m.to))

    time.sleep(2)
    proposer2.stop(fail=True)
    m.instanceID = 1
    m.value = 3
    m.to = 60000
    bytes = pickle.dumps(m)
    print '\n# ATTEMPT 3'
    s.sendto(bytes, ('localhost', m.to))

    end = time.time()

    print "Sleeping for 5s"
    time.sleep(5)
    print "Stopping proposers"
    proposer.stop()
    print "Stopping clients"
    for c in clients:
        c.stop()

    print "\nProposer 0 history: ", proposer.getHistory()
    print "Proposer 1 history: ", proposer2.getHistory()
    LOG.draw_results(print_all=True)
