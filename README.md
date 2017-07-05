# Insight-Coding-challenge-July2017 Anomaly Detection

This is an implementation of Coding Challenge for Insight Data Engineering Fellows Program starting September 2017.
It is written in Python 2.7 and tested in Python 2.7. It passes the required tests.

It requires the following modules:
 import json
 from Queue import PriorityQueue
 
## The whole process has 2 steps:

### Step 1
Read the file "batch_log.json" and initial the state of entire user network.

The entire user network is saved in a dictionary having the following structure:

    {
    'customerID1': {'amount': [], 'friend': [], 'index': []},
    'customerID2': {'amount': [], 'friend': [], 'index': []},
    'customerID3': {'amount': [], 'friend': [], 'index': []},
    ......
    }

    The 'amount' list records the purchases of customerID1.
 
 The 'friend' list contains the direct friends of customerID1. 
 
 The 'index' list records the sequence of events related to customerID1. For each event, a sequence number is assigned. For example, the first event has index number of 1, the second event has index number of 2 ...... 
 
 To save some space, only the T latest purchases will be saved in the lists, and the older events will be deleted.



For example, after reading the following events:

    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "16.83"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "59.28"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "2"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "3", "id2": "1"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "11.20"}
    {"event_type":"unfriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "3"}


The dictionary containing the entire user network is updated to:

    {
    '1': {'amount': [16.83, 59.28, 11.2], 'friend': ['2'], 'index': [1, 2, 5]},
    '2': {'amount': [], 'friend': ['1'], 'index': []},
    '3': {'amount': [], 'friend': [], 'index': []},
    }




### Step 2
Read the file "stream_log.json" line by line.
1. For each new 'purchase' event, find the D degree social network of the user. 
   
   Breadth first search algorithm is used to gather the user's friends. The user's friends are gather first (degree 1), then from his      friend find his friend of friend, and then find his friend of friend of friend ......
   
2. After the user's social nerwork is found, mean and stardard deviation are calculated.

   Here, a min-heap is used to help find the lastest purchase.
   I iterate the purchases of the user's social network and put them in the heap. The heap help keep the lastest T purchases on top.

If the new purchase is greater than mean + 3* sd, the event will be written to flagged_purchases.json.

