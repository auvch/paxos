# coding=utf-8
import log
from message import Message

LOG = log.LOG

class PaxosProposerProtocol(object):
    # 状态
    STATE_UNDEFINED = -1
    STATE_PROPOSED = 0
    STATE_AGREED = 1
    STATE_REJECTED = 2
    STATE_ACCEPTED = 3
    STATE_UNACCEPTED = 4

    def __init__(self, proposer):
        self.proposer = proposer
        self.state = PaxosProposerProtocol.STATE_UNDEFINED
        self.proposalID = (-1, -1)
        self.agreecount, self.acceptcount = (0, 0)
        self.rejectcount, self.unacceptcount = (0, 0)
        self.instanceID = -1
        self.highestseen = (0, 0)

    def propose(self, value, proposalID, instanceID):
        self.proposalID = proposalID
        self.value = value
        self.instanceID = instanceID
        message = Message(Message.MSG_PROPOSE)
        message.proposalID = proposalID
        message.instanceID = instanceID
        message.value = value
        for server in self.proposer.getAcceptors():
            message.to = server
            self.proposer.sendMessage(message)
        self.state = PaxosProposerProtocol.STATE_PROPOSED
        return self.proposalID

    def doTransition(self, message):
        # 状态转移
        if self.state == PaxosProposerProtocol.STATE_PROPOSED:
            if message.command == Message.MSG_ACCEPTOR_AGREE:
                self.agreecount += 1
                if self.agreecount >= self.proposer.getQuorumSize():
                    print "Achieved agreement quorum, last value replied was:", message.value
                    if message.value != None:  # If it's none, can do what we like. Otherwise we have to take the highest seen proposal
                        if message.sequence[0] > self.highestseen[0] or (
                                message.sequence[0] == self.highestseen[0] and message.sequence[1] > self.highestseen[
                            1]):
                            self.value = message.value
                            self.highestseen = message.sequence
                    self.state = PaxosProposerProtocol.STATE_AGREED
                    # Send 'accept' message to group
                    print "Proposer (%s) send accept to acceptors" % self.proposer.port
                    LOG.add_event({'proposalID':self.proposalID, 'type':'accept', 'value': True})
                    msg = Message(Message.MSG_ACCEPT)
                    msg.copyAsReply(message)
                    msg.value = self.value
                    msg.proposerID = msg.to
                    for s in self.proposer.getAcceptors():
                        msg.to = s
                        self.proposer.sendMessage(msg)
                    self.proposer.notifyProposer(self, message)
                return True
            if message.command == Message.MSG_ACCEPTOR_REJECT:
                self.rejectcount += 1
                if self.rejectcount >= self.proposer.getQuorumSize():
                    self.state = PaxosProposerProtocol.STATE_REJECTED
                    self.proposer.notifyProposer(self, message)
                    LOG.add_event({'proposalID':self.proposalID, 'type':'accept', 'value': False})
                return True
        if self.state == PaxosProposerProtocol.STATE_AGREED:
            if message.command == Message.MSG_ACCEPTOR_ACCEPT:
                self.acceptcount += 1
                if self.acceptcount >= self.proposer.getQuorumSize():
                    self.state = PaxosProposerProtocol.STATE_ACCEPTED
                    self.proposer.notifyProposer(self, message)
            if message.command == Message.MSG_ACCEPTOR_UNACCEPT:
                self.unacceptcount += 1
                if self.unacceptcount >= self.proposer.getQuorumSize():
                    self.state = PaxosProposerProtocol.STATE_UNACCEPTED
                    self.proposer.notifyProposer(self, message)
        pass


class PaxosAcceptorProtocol(object):
    # 状态
    STATE_UNDEFINED = -1
    STATE_PROPOSAL_RECEIVED = 0
    STATE_PROPOSAL_REJECTED = 1
    STATE_PROPOSAL_AGREED = 2
    STATE_PROPOSAL_ACCEPTED = 3
    STATE_PROPOSAL_UNACCEPTED = 4

    def __init__(self, client):
        self.client = client
        self.state = PaxosAcceptorProtocol.STATE_UNDEFINED

    def recvProposal(self, message):
        if message.command == Message.MSG_PROPOSE:
            self.proposalID = message.proposalID
            self.instanceID = message.instanceID
            # What's the highest already agreed proposal for this instance?
            (port, count) = self.client.getHighestAgreedProposal(message.instanceID)
            # Check if this proposal is numbered higher
            if count < self.proposalID[1] or (count == self.proposalID[1] and port < self.proposalID[0]):
                # Send agreed message back, with highest accepted value (if it exists)

                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED
                #                print "Agreeing to proposal: ", message.instanceID, message.value
                value = self.client.getInstanceValue(message.instanceID)
                msg = Message(Message.MSG_ACCEPTOR_AGREE)
                msg.copyAsReply(message)
                msg.value = value
                msg.sequence = (port, count)
                print "acceptor (%s) agree proposal %s with value (%s)" % (self.client.port, self.proposalID, str(value))
                LOG.add_event({'proposalID':self.proposalID, 'type':'agreement', 'acceptor':self.client.id, 'agreement': True})
                self.client.sendMessage(msg)
            else:
                # Too late, we already told someone else we'd do it
                # Send reject message, along with highest proposal id and its value
                print "acceptor (%s) reject proposal %s" % (self.client.port, self.proposalID)
                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_REJECTED
                LOG.add_event({'proposalID':self.proposalID, 'type':'agreement', 'acceptor':self.client.id, 'agreement': False})
            return self.proposalID
        else:
            pass

    def doTransition(self, message):
        if self.state == PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED and message.command == Message.MSG_ACCEPT:
            self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED
            print "acceptor (%s) accept proposal %s" % (self.client.port, self.proposalID)
            LOG.add_event({'proposalID':self.proposalID, 'type':'accepted', 'acceptor':self.client.id, 'value': True})
            # Could check on the value here, if we don't trust proposersproposer to honour what we tell them
            # send reply to proposer acknowledging
            msg = Message(Message.MSG_ACCEPTOR_ACCEPT)
            msg.copyAsReply(message)
            for l in self.client.proposers:
                msg.to = l
                self.client.sendMessage(msg)
            self.notifyClient(message)
            return True

        raise Exception("Unexpected state / command combination!")

    def notifyClient(self, message):
        self.client.notifyClient(self, message)
