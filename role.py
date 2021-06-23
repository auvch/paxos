# coding=utf-8
import threading
import time
import Queue
from message import Message, MessageHandler, AsyncMessageHandler
from record import InstanceRecord
from paxos_protocol import PaxosLeaderProtocol, PaxosAcceptorProtocol

class Acceptor(object):
    def __init__(self, port, leaders):
        self.port = port
        self.leaders = leaders
        self.instances = {}
        self.msg_handler = AsyncMessageHandler(self, self.port)
        self.failed = False

    def start(self):
        self.msg_handler.start()

    def stop(self):
        self.msg_handler.doAbort()

    def fail(self):
        self.failed = True

    def recover(self):
        self.failed = False

    def sendMessage(self, message):
        self.msg_handler.sendMessage(message)

    def recvMessage(self, message):
        if message == None:
            return
        if self.failed:
            return
        if message.command == Message.MSG_PROPOSE:
            if message.instanceID not in self.instances:
                record = InstanceRecord()
                self.instances[message.instanceID] = record
            protocol = PaxosAcceptorProtocol(self)
            protocol.recvProposal(message)
            # 更新最近见过的proposal
            self.instances[message.instanceID].addProtocol(protocol)
        else:
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)

    def notifyClient(self, protocol, message):
        if protocol.state == PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED:
            self.instances[protocol.instanceID].value = message.value

    def getHighestAgreedProposal(self, instance):
        return self.instances[instance].highestID

    def getInstanceValue(self, instance):
        return self.instances[instance].value

class Leader(object):
    def __init__(self, port, leaders=None, acceptors=None):
        self.port = port
        if leaders == None:
            self.leaders = []
        else:
            self.leaders = leaders
        if acceptors == None:
            self.acceptors = []
        else:
            self.acceptors = acceptors
        self.group = self.leaders + self.acceptors
        self.isPrimary = False
        self.proposalCount = 0
        self.msg_handler = AsyncMessageHandler(self, port)
        self.instances = {}
        self.hbListener = Leader.HeartbeatListener(self)
        self.hbSender = Leader.HeartbeatSender(self)
        self.highestInstance = 0
        self.stopped = True
        # The last time we tried to fix up any gaps
        self.lasttime = time.time()

    # ------------------------------------------------------
    # These two classes listen for heartbeats from other leaders
    # and, if none appear, tell this leader that it should
    # be the primary

    class HeartbeatListener(threading.Thread):
        def __init__(self, leader):
            self.leader = leader
            self.queue = Queue.Queue()
            self.abort = False
            threading.Thread.__init__(self)

        def newHB(self, message):
            self.queue.put(message)

        def doAbort(self):
            self.abort = True

        def run(self):
            elapsed = 0
            while not self.abort:
                s = time.time()
                try:
                    hb = self.queue.get(True, 2)
                    # Easy way to settle conflicts - if your port number is bigger than mine,
                    # you get to be the leader
                    if hb.source > self.leader.port:
                        self.leader.setPrimary(False)
                except:  # Nothing was got
                    self.leader.setPrimary(True)

    class HeartbeatSender(threading.Thread):
        def __init__(self, leader):
            self.leader = leader
            self.abort = False
            threading.Thread.__init__(self)

        def doAbort(self):
            self.abort = True

        def run(self):
            while not self.abort:
                time.sleep(1)
                if self.leader.isPrimary:
                    msg = Message(Message.MSG_HEARTBEAT)
                    msg.source = self.leader.port
                    for l in self.leader.leaders:
                        msg.to = l
                        self.leader.sendMessage(msg)

    # ------------------------------------------------------
    def sendMessage(self, message):
        self.msg_handler.sendMessage(message)

    def start(self):
        self.hbSender.start()
        self.hbListener.start()
        self.msg_handler.start()
        self.stopped = False

    def stop(self):
        self.hbSender.doAbort()
        self.hbListener.doAbort()
        self.msg_handler.doAbort()
        self.stopped = True

    def setPrimary(self, primary):
        if self.isPrimary != primary:
            # Only print if something's changed
            if primary:
                print "I (%s) am the leader" % self.port
            else:
                print "I (%s) am NOT the leader" % self.port
        self.isPrimary = primary

    # ------------------------------------------------------

    def getGroup(self):
        return self.group

    def getLeaders(self):
        return self.leaders

    def getAcceptors(self):
        return self.acceptors

    def getQuorumSize(self):
        return (len(self.getAcceptors()) / 2) + 1

    def getInstanceValue(self, instanceID):
        if instanceID in self.instances:
            return self.instances[instanceID].value
        return None

    def getHistory(self):
        return [self.getInstanceValue(i) for i in xrange(1, self.highestInstance + 1)]

    def getNumAccepted(self):
        return len([v for v in self.getHistory() if v != None])

    # ------------------------------------------------------

    def findAndFillGaps(self):
        # if no message is received, we take the chance to do a little cleanup
        for i in xrange(1, self.highestInstance):
            if self.getInstanceValue(i) == None:
                print "Filling in gap", i
                self.newProposal(0,
                                 i)  # This will either eventually commit an already accepted value, or fill in the gap with 0 or no-op
        self.lasttime = time.time()

    def garbageCollect(self):
        for i in self.instances:
            self.instances[i].cleanProtocols()

    def recvMessage(self, message):
        """Message handler will call this periodically, even if there's no message available"""
        if self.stopped:
            return
        if message == None:
            # Only run every 15s otherwise you run the risk of cutting good protocols off in their prime :(
            if self.isPrimary and time.time() - self.lasttime > 15.0:
                self.findAndFillGaps()
                self.garbageCollect()
            return
        if message.command == Message.MSG_HEARTBEAT:
            self.hbListener.newHB(message)
            return True
        if message.command == Message.MSG_EXT_PROPOSE:
            print "External proposal received at port %s" % (self.port)
            # if self.isPrimary:
            #     self.newProposal(message.value)
            self.newProposal(message.value, instance=message.instanceID)
            # else ignore - we're getting  proposals when we're not the primary
            # what we should do, if we were being kind, is reply with a message saying 'leader has changed'
            # and giving the address of the new one. However, we might just as well have failed.
            return True
        if self.isPrimary and message.command != Message.MSG_ACCEPTOR_ACCEPT:
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)
            # It's possible that, while we still think we're the primary, we'll get a
        # accept message that we're only listening in on.
        # We are interested in hearing all accepts, so we play along by pretending we've got the protocol
        # that's getting accepted and listening for a quorum as per usual
        if message.command == Message.MSG_ACCEPTOR_ACCEPT:
            if message.instanceID not in self.instances:
                self.instances[message.instanceID] = InstanceRecord()
            record = self.instances[message.instanceID]
            if message.proposalID not in record.protocols:
                protocol = PaxosLeaderProtocol(self)
                # We just massage this protocol to be in the waiting-for-accept state
                protocol.state = PaxosLeaderProtocol.STATE_AGREED
                protocol.proposalID = message.proposalID
                protocol.instanceID = message.instanceID
                protocol.value = message.value
                record.addProtocol(protocol)
            else:
                protocol = record.getProtocol(message.proposalID)
            # Should just fall through to here if we initiated this protocol instance
            protocol.doTransition(message)
        return True

    def newProposal(self, value, instance=None):
        protocol = PaxosLeaderProtocol(self)
        if instance == None:
            self.highestInstance += 1
            instanceID = self.highestInstance
        else:
            instanceID = instance
        self.proposalCount += 1
        id = (self.port, self.proposalCount)
        if instanceID in self.instances:
            record = self.instances[instanceID]
        else:
            record = InstanceRecord()
            self.instances[instanceID] = record
        protocol.propose(value, id, instanceID)
        record.addProtocol(protocol)

    def notifyLeader(self, protocol, message):
        # Protocols call this when they're done
        if protocol.state == PaxosLeaderProtocol.STATE_ACCEPTED:
            print "Protocol instance %s accepted with value %s" % (message.instanceID, message.value)
            self.instances[message.instanceID].accepted = True
            self.instances[message.instanceID].value = message.value
            self.highestInstance = max(message.instanceID, self.highestInstance)
            return
        if protocol.state == PaxosLeaderProtocol.STATE_REJECTED:
            # Look at the message to find the value, and then retry
            # Eventually, assuming that the acceptors will accept some value for
            # this instance, the protocol will complete.
            self.proposalCount = max(self.proposalCount, message.highestPID[1])
            self.newProposal(message.value)
            return True
        if protocol.state == PaxosLeaderProtocol.STATE_UNACCEPTED:
            pass