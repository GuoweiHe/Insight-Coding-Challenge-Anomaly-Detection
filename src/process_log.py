import json
import sys
try: 
    from Queue import PriorityQueue # ver. < 3.0 
except ImportError: 
    from queue import PriorityQueue


# This function reads the first file "batch_log.json" to build the initial state of the entire user network.
# The entire network is saved in a dictionary "history_log".
def initial(filename):
    history_log = {}
    inputfile = open(filename)
    firstline = json.loads(inputfile.readline())
    D = int(firstline['D'])
    T = int(firstline['T'])
    
    # The variable "index" denotes the sequence of events. 
    # For example, the first event has index of 1, the second event has index of 2 ...... 
    index = 0
    for nextline in inputfile:
        if nextline == '\n':
            continue
        new_log = json.loads(nextline)
        
        # When a new event is read in, the function "updated_log" updates the dictionary "history_log.
        index += 1
        update_log(history_log, new_log, T, index)
    
    inputfile.close()
    return (history_log, D, T, index)


# This function updates the "historty_log" with newly read in event "new_log".
def update_log(history_log, new_log, T, index):
    # If the event is "purchase", add index and purchase amount to the customer's dictionary.
    if new_log['event_type'] == 'purchase':
        ID = new_log['id']
        if not ID in history_log:
            history_log[ID] = {}
            history_log[ID]['friend'] = []
            history_log[ID]['index'] = [index]
            history_log[ID]['amount'] = [float(new_log['amount'])]
        else:
            history_log[ID]['index'].append(index)
            history_log[ID]['amount'].append(float(new_log['amount']))
        
        # To save some space, if a user has more than T purchases, we just keep T purchases and delete the older events.
        if (len(history_log[ID]['amount']) > T):
            history_log[ID]['index'].pop(0)
            history_log[ID]['amount'].pop(0)
    
    # If the event is "befriend", add the customers' IDs to each other' friend list.
    if new_log['event_type'] == 'befriend':
        ID1 = new_log['id1']
        ID2 = new_log['id2']
        if not ID1 in history_log:
            history_log[ID1] = {}
            history_log[ID1]['friend'] = []
            history_log[ID1]['index'] = []
            history_log[ID1]['amount'] = []
        if not ID2 in history_log:
            history_log[ID2] = {}
            history_log[ID2]['friend'] = []
            history_log[ID2]['index'] = []
            history_log[ID2]['amount'] = []
        history_log[ID1]['friend'].append(ID2)
        history_log[ID2]['friend'].append(ID1)
    
    # If the event is "unfriend", remove the IDs from friend list.
    if new_log['event_type'] == 'unfriend':
        ID1 = new_log['id1']
        ID2 = new_log['id2']
        if (ID1 in history_log and ID2 in history_log[ID1]['friend']):
            history_log[ID1]['friend'].remove(ID2)
        if (ID2 in history_log and ID1 in history_log[ID2]['friend']):
            history_log[ID2]['friend'].remove(ID1)


# This function read the second file "stream_log.json" line by line, and detect if the new purchase event is anomal. 
def find_anomaly(history_log, index, D, T, inputfile_name, outputfile_name):
    infile = open(inputfile_name)
    outfile = open(outputfile_name, 'w')
    for nextline in infile:
        if nextline == '\n':
            continue
        new_log = json.loads(nextline)
        
        # When a new event is read in, the function "updated_log" updates the dictionary "history_log".
        index += 1
        update_log(history_log, new_log, T, index)
    
        if new_log['event_type'] == 'purchase':
            # Call function "calculate_mean_sd" to calculate the mean and standard deviation of the user's social network.
            (mean, sd) = calculate_mean_sd(history_log, new_log['id'], D, T)
            
            # mean == -1 and sd == -1 indicate the user's social network has less than 2 purchases.
            if (mean == -1 and sd == -1):
                continue
            # If the user's purchase is greater than mean + 3 * sd, output the anomal event.
            if (float(new_log['amount']) > mean + 3 * sd):
                string = '{"event_type":"%s", "timestamp":"%s", "id": "%s", "amount": "%s", "mean": "%.2f", "sd": "%.2f"}\n' % (new_log["event_type"], new_log["timestamp"], new_log["id"], new_log["amount"], mean, sd)
                outfile.write(string)

    infile.close()
    outfile.close()
    return (index)


# Given the degree D and a customer's ID, this function returns all the friends in his social network.
def get_friend(history_log, ID, D):
    friends = set()
    current_degree_friend = set();
    current_degree_friend.add(ID)
    
    # Breadth first search algorithm is used to gather a customer's friends in his social network.
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


# This function calculate the mean and standard deviation of a given user's social network.
def calculate_mean_sd(history_log, ID, D, T):
    # A min-heap is used to help find the lastest purchase.
    # Iterate the purchases of the given user's near social network and put them in the heap.
    # The heap keep the lastest T purchases.
    priorityQueue = PriorityQueue()
    friend = get_friend(history_log, ID, D)
    for customerID in friend:
        for i in range(len(history_log[customerID]['index'])-1, -1, -1):
            # If we have less than T purchase events so far, we simply put them into the heap.
            if (priorityQueue.qsize() < T):
                priorityQueue.put(history_log[customerID]['index'][i])
            # If we have more than T purchase events, we remove the older events from the heap
            # and only keep T latest purchases in the heap.
            else:
                temp = priorityQueue.get();
                if history_log[customerID]['index'][i] < temp:
                    priorityQueue.put(temp)
                    break
                else:
                    priorityQueue.put(history_log[customerID]['index'][i])
    
    # If there are less than 2 purchases in the social network, 
    # return mean = -1 and sd = -1, which indicates no valid mean and sd results.
    if (priorityQueue.qsize() < 2):
        return (-1, -1)
    
    # Extract the indexes of the lastest T purchases from the heap.
    lastest_index = []
    while (not priorityQueue.empty()):
        lastest_index.append(priorityQueue.get())
    
    # From the indexes of the lastest T purchases, extract the expense amounts of these purchases.
    lastest_amount = []
    for customerID in friend:
        for index in lastest_index:
            if (index in history_log[customerID]['index']) :
                pos = history_log[customerID]['index'].index(index)
                lastest_amount.append(history_log[customerID]['amount'][pos])
    
    # Calculate the mean and sd, and return.
    mean = sum(lastest_amount) / len(lastest_amount)
    sd = 0
    for i in lastest_amount:
        sd += (i - mean) * (i - mean)
    sd = (sd / len(lastest_amount))**0.5
    return (mean, sd)


# Main function
def main(argv):
    # Read the file "batch_log.json" and initail the state of the entire user network.
    # The entire user network is saved in a dictionary "history_log"
    (history_log, D, T, index) = initial(argv[1])

    # Using the information we get from "batch-log.json", find the anomaly with the function "find_anomaly". 
    (index) = find_anomaly(history_log, index, D, T, argv[2], argv[3])


if __name__ == "__main__":
    main(sys.argv)
