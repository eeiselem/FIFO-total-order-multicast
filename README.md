# FIFO-total-order-multicast

simulator code given by proffesor

implement FIFO-Total (FIFO-TO) ordered multicast used in group communication. Recall that FIFO-ordered multicast requires all processes (or nodes/hosts as used in this assignment) to deliver messages in the sender's order, whereas Total-ordered multicast requires that all processes deliver all messages in exact same order (if they deliver at all). Here, you are going to implement a FIFO-Total ordered multicast using a global sequencer (as mentioned in the textbook Section 15.4.3 in 5th edition). Assume node/host with ID 0 (zero) as the sequencer. That means, once the sequence delivers a message, it instructs other processes to deliver so.

You will be using a simulator to "mimic" the process/node operations (we use the terms node, host, and process interchangeably in this assignment).

The simulator creates processes (termed as Host in the code) and enables them to send and receive messages with some arbitrary (random) channel delay/latency. Each host has two important methods/functions:

send_message(to, message) -- send message to another host/node ("to")
receive_message(frm, message, time) -- this method is called when a message is received from another host/node ("frm" is used instead of "from" because "from" is a keyword reserved in Python)
Additionally, another function, called "multicast", simply iterates over the group members as it sends the same message to all group members. 
Each message is an instance of the class Message (appears in simulation.py) that has the following main attributes/fields:

message_id
source host
destination host
message_type
payload
You can add extra/additional fields if you need them
