# coding=utf-8
from paxos_protocol import PaxosProposerProtocol

class InstanceRecord(object):
    # 记录每个 instance 的 protocols

    def __init__(self):
        self.protocols = {}
        self.highestID = (-1, -1)
        self.value = None

    def addProtocol(self, protocol):
        self.protocols[protocol.proposalID] = protocol
        if protocol.proposalID[1] > self.highestID[1] or (
                protocol.proposalID[1] == self.highestID[1] and protocol.proposalID[0] > self.highestID[0]):
            self.highestID = protocol.proposalID
            # print "update highest proposalID", protocol.proposalID

    def getProtocol(self, protocolID):
        return self.protocols[protocolID]

    def cleanProtocols(self):
        keys = self.protocols.keys()
        for k in keys:
            protocol = self.protocols[k]
            if protocol.state == PaxosProposerProtocol.STATE_ACCEPTED:
                print "Deleting protocol"
                del self.protocols[k]