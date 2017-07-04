# Insight-Coding-challenge-July2017

This is an implementation of Coding Challenge for Insight Data Engineering Fellows Program starting September 2017.
It is written in Python 2.7 and tested in Python 2.7. It passes the required tests.

It requires the following modules:
 import json
 from Queue import PriorityQueue
 
### The whole process has 2 steps:

#### Step 1
Read the file "batch_log.json" and initial the state of entire user network.

The entire user network is saved in a dictionary having the following structure:

{

    'customerID1': {'amount': [], 'friend': [u'1'], 'index': []},
