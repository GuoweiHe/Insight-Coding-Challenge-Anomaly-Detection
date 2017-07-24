import json
import sys
try: 
    from Queue import PriorityQueue # ver. < 3.0 
except ImportError: 
    from queue import PriorityQueue


## This function reads in the first file "batch_log.json" to build the initial state of the entire user network.
def initialize(filename):
    # The user network is stored into a Python dict "history_log".
    history_log = {}
    inputfile = open(filename)
    firstline = json.loads(inputfile.readline())
    D = int(firstline['D'])
    T = int(firstline['T'])
    
    # For each event, an index number is assigned. 
    # For example, the first event has index of 1, the second event has index of 2, and so on. 
    index = 0
    for nextline in inputfile:
        if nextline == '\n':
            continue
        new_event = json.loads(nextline)
        
        # When a new event is read in, call the function "update_log" to update the dict "history_log".
        index += 1
        update_log(history_log, new_event, T, index)
    
    inputfile.close()
    return (history_log, D, T, index)


## This function updates the dict "historty_log" with newly read in event.
def update_log(history_log, new_event, T, index):
    # If the event type is "purchase", add index number and purchase amount to the dict.
    if new_event['event_type'] == 'purchase':
        ID = new_event['id']
        # Check if the dictionary has the ID.
        if not ID in history_log:
            history_log[ID] = {}
            history_log[ID]['amount'] = []
            history_log[ID]['index'] = []
            history_log[ID]['friend'] = []
        
        # Add index number and purchase amount for the ID.
        history_log[ID]['amount'].append(float(new_event['amount']))
        history_log[ID]['index'].append(index)
        
        # To save some disk space, if a user has more than T purchases, we just keep the latest T purchases and delete the older purchases.
        if (len(history_log[ID]['amount']) > T):
            history_log[ID]['amount'].pop(0)
            history_log[ID]['index'].pop(0)
    
    # If the event type is "befriend", add the user IDs to each other's friend list.
    if new_event['event_type'] == 'befriend':
        ID1 = new_event['id1']
        ID2 = new_event['id2']
        if not ID1 in history_log:
            history_log[ID1] = {}
            history_log[ID1]['amount'] = []
            history_log[ID1]['index'] = []
            history_log[ID1]['friend'] = []
        if not ID2 in history_log:
            history_log[ID2] = {}
            history_log[ID2]['amount'] = []
            history_log[ID2]['index'] = []
            history_log[ID2]['friend'] = []
        history_log[ID1]['friend'].append(ID2)
        history_log[ID2]['friend'].append(ID1)
    
    # If the event type is "unfriend", remove the IDs from friend lists.
    if new_event['event_type'] == 'unfriend':
        ID1 = new_event['id1']
        ID2 = new_event['id2']
        if (ID1 in history_log and ID2 in history_log[ID1]['friend']):
            history_log[ID1]['friend'].remove(ID2)
        if (ID2 in history_log and ID1 in history_log[ID2]['friend']):
            history_log[ID2]['friend'].remove(ID1)


## This function read in the second file "stream_log.json" line by line, and determine if the new purchase is anormal. 
def find_anomaly(history_log, index, D, T, inputfile_name, outputfile_name):
    infile = open(inputfile_name)
    outfile = open(outputfile_name, 'w')
    for nextline in infile:
        if nextline == '\n':
            continue
        new_event = json.loads(nextline)
        
        # When a new event is read in, call the function "updated_log" to update the dict "history_log".
        index += 1
        update_log(history_log, new_event, T, index)
    
        if new_event['event_type'] == 'purchase':
            # Call function "calculate_mean_sd" to calculate the mean and sd of the user's social network.
            (mean, sd) = calculate_mean_sd(history_log, new_event['id'], D, T)
            
            # mean == -1 and sd == -1 indicate that the user's social network has less than 2 purchases.
            if (mean == -1 and sd == -1):
                continue
            # If the user's purchase amount is greater than mean + 3 * sd, output the abnormal purchase.
            if (float(new_event['amount']) > mean + 3 * sd):
                string = '{"event_type":"%s", "timestamp":"%s", "id": "%s", "amount": "%s", "mean": "%.2f", "sd": "%.2f"}\n' % (new_event["event_type"], new_event["timestamp"], new_event["id"], new_event["amount"], mean, sd)
                outfile.write(string)

    infile.close()
    outfile.close()
    return (index)


## Given a degree D and a customer's ID, this function returns all the friends in the user's Dth degree social network.
def get_friends(history_log, ID, D):
    friends = set()
    current_degree_friend = set();
    current_degree_friend.add(ID)
    
    # Breadth First Search algorithm is used to find all the friends in the user's Dth degree social network.
    while (D > 0):
        next_degree_friend = set();
        for i in current_degree_friend:
            for j in history_log[i]['friend']:
                if (j != ID and not j in friends and not j in current_degree_friend):
                    next_degree_friend.add(j)
        friends |= next_degree_friend
        current_degree_friend = next_degree_friend
        D -= 1
        
    return list(friends)


## This function calculates the mean and standard deviation of a given user's Dth degree social network.
def calculate_mean_sd(history_log, ID, D, T):
    # Find the friends in the user's Dth degree social network.
    friends = get_friends(history_log, ID, D)
    length = len(friends)
    
    # Since all user's purchase lists have already sorted from old to new, 
    # when we search the latest purchases, we can start from the end of the lists.
    position = []
    for i in range(length):
        position.append(len(history_log[friends[i]]['index']) - 1)  
    latest_amount = []
    
    while True:
        max_index = -1
        max_list = -1
        # For each friend's index list, find the latest index and the index list with this largest index.
        for i in range(length):
            if (position[i] >= 0 and history_log[friends[i]]['index'][position[i]] > max_index):
                max_index = history_log[friends[i]]['index'][position[i]]
                max_list = i
        # If there is no more event or we have already get the latest T purchases, exit the while loop
        if (max_list == -1 or len(latest_amount) == T):
            break;
        # If find the next latest purchase, put the purchase amount in the list 'latest_amout'.
        latest_amount.append(history_log[friends[max_list]]['amount'][position[max_list]])
        position[max_list] -= 1
            
    # If there are less than 2 purchases in the social network, return mean = -1 and sd = -1.
    if (len(latest_amount) < 2):
        return (-1, -1)
    
    # Calculate the mean and sd, and return.
    mean = sum(latest_amount) / len(latest_amount)
    sd = 0
    for i in latest_amount:
        sd += (i - mean) * (i - mean)
    sd = (sd / len(latest_amount))**0.5
    return (mean, sd)


## This function calculates the mean and standard deviation of a given user's Dth degree social network.
## This function use a priority queue to improve the performace.
def calculate_mean_sd_with_heap(history_log, ID, D, T):
    # Find the friends in the user's Dth degree social network.
    friends = get_friends(history_log, ID, D)
    length = len(friends)
    
    # Use a priority queue to find the latest T pruchasese.
    class Element:
        def __init__(self, index, list_pos, list_index):
            self.index = index
            self.list_pos = list_pos
            self.list_index = list_index
            return
        def __cmp__(self, other):
            return -cmp(self.index, other.index)
    
    pqueue = PriorityQueue()
    for i in range(length):
        length = len(history_log[friends[i]]['index'])
        if (length > 0):
            pqueue.put(Element(history_log[friends[i]]['index'][-1], length-1, i))
  
    latest_amount = []
    
    while True:
        if (pqueue.empty() or len(latest_amount) == T):
            break;
        e = pqueue.get();
        if (e.list_pos > 0):
            pqueue.put(Element(history_log[friends[e.list_index]]['index'][e.list_pos - 1], e.list_pos - 1, e.list_index))       
        latest_amount.append(history_log[friends[e.list_index]]['amount'][e.list_pos])
    
            
    # If there are less than 2 purchases in the social network, return mean = -1 and sd = -1.
    if (len(latest_amount) < 2):
        return (-1, -1)
    
    # Calculate the mean and sd, and return.
    mean = sum(latest_amount) / len(latest_amount)
    sd = 0
    for i in latest_amount:
        sd += (i - mean) * (i - mean)
    sd = (sd / len(latest_amount))**0.5
    return (mean, sd)


## Main function
def main(argv):
    # Step 1: 
    # Read in the file "batch_log.json" and initialize the entire user network.
    # I tidy the data in the file, and save it in a Python dictionary data structure.
    # The user network is stored into a Python dict "history_log"
    # The input of this function is file name. The output of this function is the Python dictionary, degree, 
    # the number of consecutive purchases, and the index of the last event.
    (history_log, D, T, index) = initialize(argv[1])
    
    # Step 2:
    # Read in file "stream_log.json" line by line, find the abnormal purchase.
    # The input of this function is the dictionary, the degree, the number of consecutive purchase, and the input and output files.
    (index) = find_anomaly(history_log, index, D, T, argv[2], argv[3])


if __name__ == "__main__":
    print "Program is running ..."
    main(sys.argv)
