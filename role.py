# coding=utf-8
import threading
import time
import Queue
import log
from message import Message, MessageHandler, AsyncMessageHandler
from record import InstanceRecord
from paxos_protocol import PaxosLeaderProtocol, PaxosAcceptorProtocol

LOG = log.LOG

class Acceptor(object):
    def __init__(self, port, id, leaders):
        self.port = port
        self.id = id # start from 0
        self.leaders = leaders
        self.instances = {}
        self.msg_handler = AsyncMessageHandler(self, self.port)
        self.failed = False
        LOG.set_acceptor_live(self.id, True)

    def start(self):
        self.msg_handler.start()

    def stop(self):
        self.msg_handler.doAbort()

    def fail(self):
        self.failed = True
        LOG.set_acceptor_live(self.id, False)

    def recover(self):
        self.failed = False
        LOG.set_acceptor_live(self.id, True)

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
            LOG.add_event({"type":"result", "acceptor":self.id, "proposalID":message.proposalID, "accepted":True, "value":message.value})

    def getHighestAgreedProposal(self, instance):
        return self.instances[instance].highestID

    def getInstanceValue(self, instance):
        return self.instances[instance].value

class Leader(object):
    def __init__(self, port, id, leaders=None, acceptors=None):
        self.port = port
        self.id = id # start from 0
        if leaders == None:
            self.leaders = []
        else:
            self.leaders = leaders
        if acceptors == None:
            self.acceptors = []
        else:
            self.acceptors = acceptors
        self.group = self.leaders + self.acceptors
        self.proposalCount = 0
        self.msg_handler = AsyncMessageHandler(self, port)
        self.instances = {}
        self.highestInstance = 0
        self.stopped = True
        # The last time we tried to fix up any gaps
        self.lasttime = time.time()

    def sendMessage(self, message):
        self.msg_handler.sendMessage(message)

    def start(self):
        self.msg_handler.start()
        self.stopped = False

    def stop(self):
        self.msg_handler.doAbort()
        self.stopped = True

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
            return
        if message.command == Message.MSG_EXT_PROPOSE:
            print "External proposal received at port %s" % (self.port)
            self.newProposal(message.value, instance=message.instanceID)
            return True
        if message.command != Message.MSG_ACCEPTOR_ACCEPT:
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)
        if message.command == Message.MSG_ACCEPTOR_ACCEPT:
            if message.instanceID not in self.instances:
                self.instances[message.instanceID] = InstanceRecord()
            record = self.instances[message.instanceID]
            if message.proposalID not in record.protocols:
                protocol = PaxosLeaderProtocol(self)
                protocol.state = PaxosLeaderProtocol.STATE_AGREED
                protocol.proposalID = message.proposalID
                protocol.instanceID = message.instanceID
                protocol.value = message.value
                record.addProtocol(protocol)
            else:
                protocol = record.getProtocol(message.proposalID)
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
        LOG.add_proposal(id)
        LOG.add_event({"type":"propose", "proposer":self.id, "proposalID":id})
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
            LOG.add_event({"type":"result", "proposer":self.id, "proposalID":message.proposalID, "accepted":True, "value":message.value})
            self.instances[message.instanceID].accepted = True
            self.instances[message.instanceID].value = message.value
            self.highestInstance = max(message.instanceID, self.highestInstance)
            return
        if protocol.state == PaxosLeaderProtocol.STATE_REJECTED:
            # Look at the message to find the value, and then retry
            # Eventually, assuming that the acceptors will accept some value for
            # this instance, the protocol will complete.
            self.proposalCount = max(self.proposalCount, message.highestPID[1])
            #LOG.add_event({"type":"result", "proposalID":(self.port, self.proposalCount), "accepted":False})
            self.newProposal(message.value)
            return True
        if protocol.state == PaxosLeaderProtocol.STATE_UNACCEPTED:
            pass