# Insight Coding Challenge: Anomaly Detection

This is an implementation of the coding challenge for Insight Data Engineering Fellows Program starting September 2017.

The code is written and tested in Python 2.7. It requires the `json` module.


The whole process contains 2 steps. 

### Step 1: Initilization

Read the file `batch_log.json` and initialize the state of the entire user network.

The entire user network is stored into a `dict` having the following structure:

    {
    'customerID1': {'amount': [], 'friend': [], 'index': []},
    'customerID2': {'amount': [], 'friend': [], 'index': []},
    'customerID3': {'amount': [], 'friend': [], 'index': []},
    ......
    }

    The 'amount' list records the purchases of customerID1.
    The 'friend' list contains the direct friends of customerID1. 
    The 'index' list records the sequence of events related to customerID1. 
    For each event, a sequence number is assigned. For example, the first event has index number of 1, the second event has index number of 2 ...... 
    To save some space, only the T latest purchases will be saved in the lists, and the older events will be deleted.


For example, after importing the following events:

    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "16.83"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "59.28"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "2"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "3", "id2": "1"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "11.20"}
    {"event_type":"unfriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "3"}

The `dict` containing the entire user network is updated to:

    {
    '1': {'amount': [16.83, 59.28, 11.2], 'friend': ['2'], 'index': [1, 2, 5]},
    '2': {'amount': [], 'friend': ['1'], 'index': []},
    '3': {'amount': [], 'friend': [], 'index': []},
    }

### Step 2: Anomaly Detection

Read the file `stream_log.json` line by line.

2.1 For each new purchase, find the `D`th degree social network of the user. 
   
   Breadth First Search algorithm is used to search the social network. First, gather the user's direct friends. Then, from the user's direct friend, find the user's "friends of friends", and then find the users's "friends of friends of friends" and so on.
   
2.2 From the user's `D`th degree social network, find the latest `T` purchases, and calculate the `mean` and `sd`.

   Since all user's purchase list has already been sorted from old to new, when we search the latest purchases, we can just start from the end of the list. We stop the search until we obtain latest `T` purchase or there is no more purchase.

2.3 If the new purchase amount is greater than `mean + 3 * sd`, the purchase will be written to `flagged_purchases.json`.

