# coding=utf-8
import random
import threading
import pickle
import socket
import Queue

class Message(object):
    MSG_PROPOSE = 0
    MSG_ACCEPTOR_AGREE = 1
    MSG_ACCEPTOR_REJECT = 2
    MSG_ACCEPT = 3
    MSG_ACCEPTOR_ACCEPT = 4
    MSG_ACCEPTOR_UNACCEPT = 5
    MSG_EXT_PROPOSE = 6
    MSG_HEARTBEAT = 7

    def __init__(self, command=None):
        self.command = command

    def copyAsReply(self, message):
        self.proposalID = message.proposalID
        self.instanceID = message.instanceID
        self.to = message.source
        self.source =  message.to
        self.value = message.value


class MessageHandler(threading.Thread):
    # 负责 socket 消息传输，并把消息传递给节点

    class MessageListener(threading.Thread):

        def __init__(self, owner):
            self.owner = owner
            threading.Thread.__init__(self)

        def run(self):
            while not self.owner.abort:
                try:
                    (bytes, addr) = self.owner.socket.recvfrom(2048)
                    msg = pickle.loads(bytes)
                    msg.source = addr[1]
                    self.owner.queue.put(msg)
                except:
                    pass

    def __init__(self, owner, port, timeout=2):
        self.owner = owner
        threading.Thread.__init__(self)
        self.abort = False
        self.timeout = 2
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 200000)
        self.socket.bind(("localhost", port))
        self.socket.settimeout(timeout)
        self.queue = Queue.Queue()
        self.helper = MessageHandler.MessageListener(self)

    def run(self):
        self.helper.start()
        while not self.abort:
            message = self.waitForMessage()
            self.owner.recvMessage(message)

    def waitForMessage(self):
        try:
            msg = self.queue.get(True, 3)
            return msg
        except:
            return None

    def sendMessage(self, message):
        bytes = pickle.dumps(message)
        address = ("localhost", message.to)
        self.socket.sendto(bytes, address)
        return True

    def doAbort(self):
        self.abort = True

class AsyncMessageHandler(MessageHandler):
    """randomly delays messages and delivers them in arbitrary orders"""

    def __init__(self, owner, port, timeout=2):
        MessageHandler.__init__(self, owner, port, timeout)
        self.messages = set()

    def waitForMessage(self):
        try:
            msg = self.queue.get(True, 0.1)
            self.messages.add(msg)
        except:  # ugh, specialise the exception!
            pass
        if len(self.messages) > 0 and random.random() < 0.95:  # Arbitrary!
            msg = random.choice(list(self.messages))
            self.messages.remove(msg)
        else:
            msg = None
        return msg