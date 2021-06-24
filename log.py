# coding=utf-8
#O: 消息送达
#_：acceptor fail
#V：accept/agree
#X：disagree

import copy
import util

class Log(object):
    def __init__(self):
        self.proposals = list()
        self.start_pointer = 0
        self.num_proposer = 0
        self.num_acceptor = 0
        self.acceptors_live = list()
        self.proposers_live = list()
        self.proposer_results = list()
        self.acceptor_results = list()

    def set_proposer(self, number):
        self.num_proposer = number
        self.proposers_live = [True for _ in range(number)]
        self.proposer_results = [None for _ in range(number)]

    def set_acceptor(self, number):
        self.num_acceptor = number
        self.acceptors_live = [True for _ in range(number)]
        self.acceptor_results = [None for _ in range(number)]

    def add_proposal(self, proposalID):
        tmp_dict = util.search_dict_list(self.proposals, 'id', proposalID)
        if tmp_dict is not None:
            return
        self.proposals.append({"id":proposalID, "agreements":[False for _ in range(self.num_acceptor)],
            "accepted":[False for _ in range(self.num_acceptor)]})#,"results_accepted":[False for _ in range(self.num_proposer)]})
    
    def add_event(self, event):
        tmp_dict = util.search_dict_list(self.proposals, 'id', event['proposalID'])
        if tmp_dict is None:
            self.add_proposal(event['proposalID'])
            tmp_dict = util.search_dict_list(self.proposals, 'id', event['proposalID'])
            print("ERROR: proposal not in list", event['proposalID'])
        if event['type'] == "propose":
            tmp_dict['proposer'] = event['proposer']
            tmp_dict['proposers_live'] = copy.deepcopy(self.proposers_live)
        if event['type'] == "agreement":
            tmp_dict['agreements'][event['acceptor']] = event['agreement']
            tmp_dict['acceptors_live'] = copy.deepcopy(self.acceptors_live)
        if event['type'] == "accept":
            tmp_dict['accept'] = event['value']
        if event['type'] == "accepted":
            tmp_dict['accepted'][event['acceptor']] = event['value']
        if event['type'] == "result":
            if event['accepted']:
                if 'proposer' in event:
                    self.proposer_results[event['proposer']] = event['value']
                if 'acceptor' in event:
                    self.acceptor_results[event['acceptor']] = event['value']
    
    def set_acceptor_live(self, acceptor_id, live):
        self.acceptors_live[acceptor_id] = live

    def set_proposer_live(self, proposer_id, live):
        self.proposers_live[proposer_id] = live

    def draw_results(self, draw_header=False, print_all=False):
        if print_all:
            draw_header = True
            self.start_pointer = 0
        if draw_header:
            print("\nProposer             Acceptor")
        for event_idx in range(self.start_pointer, len(self.proposals)):
            output = "  "
            for _ in range(self.num_proposer):
                output += "|  "
            output += "   "
            for _ in range(self.num_acceptor):
                output += "    |"
            print(output)

            output = "  "
            for i in range(self.proposals[event_idx]['proposer']):
                if self.proposals[event_idx]['proposers_live'][i]:
                    output += "|  "
                else:
                    output += "_  "
            output += "O"
            for i in range(self.proposals[event_idx]['proposer']+1, self.num_proposer):
                if self.proposals[event_idx]['proposers_live'][i]:
                    output += "--|"
                else:
                    output += "--_"
            output += "-----"
            for i in range(self.num_acceptor):
                if self.acceptors_live[i]:
                    output += "--->O"
                else:
                    output += "--->_"
            output += "  Proposal ID: "
            output += str(self.proposals[event_idx]['id'])
            print(output)

            output = "  "
            for i in range(self.proposals[event_idx]['proposer']):
                if self.proposals[event_idx]['proposers_live'][i]:
                    output += "|  "
                else:
                    output += "_  "
            output += "O<"
            for i in range(self.proposals[event_idx]['proposer']+1, self.num_proposer):
                if self.proposals[event_idx]['proposers_live'][i]:
                    output += "-|-"
                else:
                    output += "-_-"
            output += "----"
            for i in range(self.num_acceptor):
                if self.acceptors_live[i]:
                    if self.proposals[event_idx]['agreements'][i]:
                        output += "----V"
                    else:
                        output += "----X"
                else:
                    output += "----_"
            print(output)

            if 'accept' in self.proposals[event_idx]:
                output = "  "
                for i in range(self.proposals[event_idx]['proposer']):
                    if self.proposals[event_idx]['proposers_live'][i]:
                        output += "|  "
                    else:
                        output += "_  "
                output += "O"
                for i in range(self.proposals[event_idx]['proposer']+1, self.num_proposer):
                    if self.proposals[event_idx]['proposers_live'][i]:
                        output += "--|"
                    else:
                        output += "--_"
                output += "-----"
                for i in range(self.num_acceptor):
                    if self.acceptors_live[i]:
                        output += "--->O"
                    else:
                        output += "--->_"
                output += "  Accept?"
                print(output)

                output = "  "
                for i in range(self.proposals[event_idx]['proposer']):
                    if self.proposals[event_idx]['proposers_live'][i]:
                        output += "|  "
                    else:
                        output += "_  "
                output += "O<"
                for i in range(self.proposals[event_idx]['proposer']+1, self.num_proposer):
                    if self.proposals[event_idx]['proposers_live'][i]:
                        output += "-|-"
                    else:
                        output += "-_-"
                output += "----"
                for i in range(self.num_acceptor):
                    if self.acceptors_live[i]:
                        if self.proposals[event_idx]['accepted'][i]:
                            output += "----V"
                        else:
                            output += "----X"
                    else:
                        output += "----_"
                print(output)
            
            output = ""
            for i in range(self.num_proposer):
                output += " "
                if self.proposals[event_idx]['proposers_live'][i]:
                    result = str(self.proposer_results[i])
                else:
                    result = ""
                for _ in range(3-len(result)):
                    result += " "
                output += result
            output += "   "
            for i in range(self.num_acceptor):
                if self.acceptor_results[i] is None:
                    result = ""
                else:
                    result = str(self.acceptor_results[i])
                result = " " + result
                for _ in range(5-len(result)):
                    result = " " + result
                #output += " "
                output += result
            output += "  Value"

            print(output)


            self.start_pointer += 1
        return

LOG = Log()


_allowed_symbols = [
    'LOG'
]
