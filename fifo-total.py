#!/bin/python3

### TODO:
# Author name: Elliott Eisele-Miller

import heapq 
from simulator import *

# Multicast events for the driver
multicast_events = [
    #(time, message_id, sending host_id, message/payload)
    # my code should work with various times   
    (10, 'M1', 1, 'January'),
    (20, 'M2', 1, 'February'),
    (30, 'M3', 1, 'March'),
    (10, 'M4', 2, 'One'),
    (20, 'M5', 2, 'Two'),
    (30, 'M6', 2, 'Three')
]

class Host(Node):
    def __init__(self, sim, host_id):
        Node.__init__(self, sim, host_id)
        self.host_id = host_id
        self.gmembers = []
        self.is_sequencer = (self.host_id == 0)

        # Common state for all hosts
        self.sender_seq_num = 0 # Own sequence number for messages being sent by this host
        # Holds messages waiting for global sequence number
        self.hold_back_queue = {} # Key: (original_sender_id, original_sender_seq), Value: message
        # for if order message arrives before the message from non sequencer host
        self.buffered_orders = {} # Key: (original_sender_id, original_sender_seq), Value: message
        # will use min heapq/priority queue to hold and sort messages that are ready to be delivered once the previous message is delivered, have global sequence number
        self.ready_to_deliver = [] # stores in order as [(global_seq, message)]
        self.next_global_seq_to_deliver = 1 # Counter for delivering from ready_to_deliver

        # Sequencer-specific state 
        self.global_seq_num = 0 # Next global sequence number Sequencer will assign
        # Needed at sequencer to enforce sender FIFO before assigning global_seq
        self.expected_seq = {} # key: sender_id, value: next expected sender_seq from that sender
        # Buffers messages at the sequencer if they arrive out of sender order
        self.sender_buffer = {} # key: sender_seq, value: message}

    def initialize(self):
        # only sequencer needs these data structures, I'm not sure why there is a separate initialization function but I added this bit here
        if self.is_sequencer:
            # Sequencer needs to track expected sequences for all potential senders (all members)
            for member in self.gmembers:
                 sender_id = member.host_id
                 self.expected_seq[sender_id] = 0 # Initialize expected sequence number for each sender to 0
                 self.sender_buffer[sender_id] = {} # Initialize sender buffer for each sender

    def multicast(self, time, message_id, message_type, payload):
        # Multicast message to the group
        print(f'Time {time:4}:: {self} SENDING mulitcast message [{message_id}]')

        # Create message and send to all members of the group including itself
        # TODO: put sequence numbers and so other things
        current_sender_seq = self.sender_seq_num
        self.sender_seq_num += 1 # Increment sequence number for the next message this host sends

        if message_type == 'ORDER': # order message from the sequencer.
             # payload expected to be a dictionary containing global_seq, original_sender, 
             # original_sender_seq, and original_message_id could have a structure
             # original_message_id and called here but wanted to keep structure consistent
             full_payload = payload # Use the provided payload directly
        else:
             full_payload = {
                'original_payload': payload, # The actual data (e.g., 'Januray')
                'sender_seq': current_sender_seq, # This host's sequence number for this message
                'original_sender': self.host_id, # This host's ID
                'message_id': message_id # The original message ID from the driver event
             }

        for to in self.gmembers:
            mcast = Message(message_id, self, to, message_type, full_payload)
            self.send_message(to, mcast)

    def receive_message(self, frm, message, time):
        # This function is called when a message is received by this host (self)
        # frm --- from which host/node the message is came (source of the message)
        # message -- message that is received
        # time -- the time when the message is received (the current time)
        print(f'Time {time:4}:: {self} RECEIVED message [{message.message_id}] from {frm}')

        if self.is_sequencer:
            if message.mtype != 'ORDER':
                sender_id = frm.host_id 
                sender_seq = message.payload['sender_seq']
                original_message_id = message.payload['message_id']
                if sender_seq == self.expected_seq[sender_id]:
                    # Message is the next expected message from this sender according to sender-FIFO
                    self.global_seq_num += 1 # Assign the next global sequence number
                    global_sequence_number = self.global_seq_num

                    order_payload = {
                        'global_seq': global_sequence_number,
                        'original_sender': sender_id,
                        'original_sender_seq': sender_seq,
                        'original_message_id': original_message_id,
                    }

                    self.multicast(time, original_message_id, 'ORDER', order_payload)

                    # Passes the original DRIVER_MCAST message object to deliver_message so deliver_message can print proper format
                    message.payload = message.payload['original_payload']
                    self.deliver_message(time, message)

                    self.expected_seq[sender_id] += 1 # Move to the next expected sequence from this sender

                    while True: # Check buffer for messages from this sender that are now in order and ready to be delivered
                        next_expected = self.expected_seq[sender_id]
                        if next_expected in self.sender_buffer[sender_id]:
                            buffered_message = self.sender_buffer[sender_id].pop(next_expected) # Get the original message object from buffer
                            buffered_original_message_id = buffered_message.payload['message_id']
                            # the rest of this is just like the above
                            self.global_seq_num += 1
                            global_sequence_number = self.global_seq_num

                            order_payload = {
                                'global_seq': global_sequence_number,
                                'original_sender': sender_id,
                                'original_sender_seq': next_expected,
                                'original_message_id': buffered_original_message_id,
                            }
 
                            self.multicast(time, buffered_original_message_id, 'ORDER', order_payload)
                            # Deliver the buffered message after chaning payload format
                            buffered_message.payload = buffered_message.payload['original_payload']
                            self.deliver_message(time, buffered_message)

                            self.expected_seq[sender_id] += 1
                        else: # No more consecutive messages in buffer for this sender     
                            break
                else: # Message is out of sender-FIFO order, buffer the original message object                   
                    # print(f'Time {time:4}:: Sequencer/Node-0 buffering out-of-order message [{original_message_id}] from {frm} (expected: {self.expected_seq[sender_id]}, received: {sender_seq})')
                    self.sender_buffer[sender_id][sender_seq] = message # Buffer the original message object


        else: # Regular host   
            if message.mtype != 'ORDER': # hold back for the corresponding ORDER message.
                sender_id = frm.host_id 
                sender_seq = message.payload['sender_seq']
                original_message_id = message.payload['message_id']

                # Use the original sender and its sequence number as the key for the hold-back queue
                key = (sender_id, sender_seq)
                # print(f'Time {time:4}:: {self} added message [{original_message_id}] to holdback queue with key {key}') 
                self.hold_back_queue[key] = message # Store the message 

                if key in self.buffered_orders: # for if order message arrives before the original message, if in buffer an order message for it has already arrived   
                    # print(f'Time {time:4}:: {self} Found buffered ORDER for [{original_message_id}] (key {key}). Processing buffered order.')
                    buffered_order_payload = self.buffered_orders.pop(key) # Retrieve the buffered order message payload
                    global_seq = buffered_order_payload['global_seq']
                    # Don't need to retrieve the message from the hold-back queue since its already local
                    heapq.heappush(self.ready_to_deliver, (global_seq, message)) # Add to ready-to-deliver queue

                    # Immediately try to deliver from the ready queue
                    while self.ready_to_deliver and self.ready_to_deliver[0][0] == self.next_global_seq_to_deliver:
                        # Pop the next message in global sequence order from the heap
                        _, message_to_deliver = heapq.heappop(self.ready_to_deliver)

                        # converting payload to original format for delivery
                        message_to_deliver.payload = message_to_deliver.payload['original_payload']
                        self.deliver_message(time, message_to_deliver) 
                        self.next_global_seq_to_deliver += 1

            else: # Received an order message from the sequencer (Host-0)           
                # print(f'Time {time:4}:: {self} RECEIVED ORDER message [{message.message_id}] from {frm}') 
                original_sender_seq = message.payload['original_sender_seq']
                original_message_id = message.payload['original_message_id']
                global_seq = message.payload['global_seq']
                original_sender = message.payload['original_sender']

                # key to find the corresponding original message in the hold-back queue
                key_to_find = (original_sender, original_sender_seq)

                if key_to_find in self.hold_back_queue:
                    # print(f'Time {time:4}:: {self} Found message with key {key_to_find} in holdback queue.')
                    held_message = self.hold_back_queue.pop(key_to_find)
                    # Add the message (with its assigned global sequence) to the ready-to-deliver queue
                    heapq.heappush(self.ready_to_deliver, (global_seq, held_message))
                    # print(f'Time {time:4}:: {self} Added message [{original_message_id}] (global_seq {global_seq}) to ready-to-deliver queue.')

                    # Attempt to deliver any consecutive messages from the ready_to_deliver queue
                    while self.ready_to_deliver and self.ready_to_deliver[0][0] == self.next_global_seq_to_deliver:
                        # Pop the next message in global sequence order from the heap
                        _, message_to_deliver = heapq.heappop(self.ready_to_deliver)
                        # print(f'Time {current_time:4}:: {self} Attempting delivery for global_seq {seq_to_deliver}, next expected {self.next_global_seq_to_deliver}')

                        message_to_deliver.payload = message_to_deliver.payload['original_payload']
                        self.deliver_message(time, message_to_deliver) 

                        self.next_global_seq_to_deliver += 1

                else: # This means the ORDER message arrived before the corresponding message from original sender                   
                    # The message will be put in the hold_back_queue when it arrives.
                    # print(f'Time {time:4}:: {self} Message with key {key_to_find} (for [{original_message_id}] global_seq {global_seq}) NOT found in holdback queue. Original message may not have arrived yet.')
                    self.buffered_orders[key_to_find] = message.payload 

    def deliver_message(self, time, message):
        print(f'Time {time:4}:: {self} DELIVERED message [{message.message_id}] -- {message.payload}')


# Driver: you DO NOT need to change anything here
class Driver:
    def __init__(self, sim):
        self.hosts = []
        self.sim = sim

    def run(self, nhosts=3):
        for i in range(nhosts):
            host = Host(self.sim, i)
            self.hosts.append(host)

        for host in self.hosts:
            host.gmembers = self.hosts
            host.initialize()

        for event in multicast_events:
            time = event[0]
            message_id = event[1] # 'M1', 'M2', etc.
            message_type = 'DRIVER_MCAST' # initial message from the driver
            host_id = event[2] # The host originating this specific message (which node)
            payload = event[3] # 'Januray', 'One', etc.
            self.sim.add_event(Event(time, self.hosts[host_id].multicast, time, message_id, message_type, payload))

def main():
    # Create simulation instance
    sim = Simulator(debug=False, random_seed=1233)

    # Start the driver and run for nhosts (should be >= 3)
    driver = Driver(sim)
    driver.run(nhosts=5) 

    # Start the simulation
    sim.run()

if __name__ == "__main__":
    main()