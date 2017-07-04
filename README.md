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

    'customerID1': {'amount': [], 'friend': [], 'index': []},
    'customerID2': {'amount': [], 'friend': [], 'index': []},
    'customerID3': {'amount': [], 'friend': [], 'index': []},
    ......

For example, after reading the following events:

 {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "16.83"}
 {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "59.28"}

{"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "2"}

{"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "3", "id2": "1"}

{"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "11.20"}

{"event_type":"unfriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "3"}


The dictionary containing the entire user network is updated to:

    '1': {'amount': [16.83, 59.28, 11.2], 'friend': ['2'], 'index': [1, 2, 5]},
    '2': {'amount': [], 'friend': ['1'], 'index': []},
    '2': {'amount': [], 'friend': [], 'index': []},
    ......
