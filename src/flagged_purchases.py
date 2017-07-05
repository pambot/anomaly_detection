import argparse
import json
import networkx as nx
import numpy as np
from datetime import datetime
from collections import OrderedDict


class CustomerNetwork(object):
    """A custom class for holding the social network customer connections in the 
    form of a graph and the purchases in the form of a dictionary of heaps.
    
    Parameters
    ----------
    friends : Networkx.Graph
        The social network graph where nodes are personal IDs.
    
    purchases : data_structures.Heap
        A `dict` where the keys are IDs, and the values are tuples of the form
        (timestamp, amount). Timestamps are of `datetime.datetime` objects and
        `amount` is a `float`.
    
    d : int
        The degree of separation to search for purchases.
    
    t : int
        The number of most recent purchases for statistical consideration when
        calculating anomalies.
    
    s : int
        The threshold for number of standard deviations above the mean to flag.
    
    batch_file : str
        The path and filename for the batch log.
    
    stream_file : str
        The path and filename for the stream log.
    
    flag_file : str
        The path and filename for the flag log.
    
    invalid_file : str
        The path and filename for the invalid entries log.
    
    """
    def __init__(self, d=None, t=None, s=3, batch_file=None, stream_file=None, flag_file=None, invalid_file=None):
        self.friends = nx.Graph()
        self.purchases = {}
        self.batch_file = batch_file
        self.stream_file = stream_file
        self.flag_file = flag_file
        self.invalid_file = invalid_file
        self.d = d
        self.t = t
        self.s = int(s)
        self._seen = set([])
        self._index = 0
    
    def _log_invalid_entry(self, entry):
        """Dump invalid entries into a log file.
        """
        l = str(entry)
        with open(self.invalid_file, 'a') as f:
            f.write(l + '\n')
        return
    
    def _validate_log_and_decode(self, log_str):
        """This makes sure that each entry is valid. Each entry must be correct
        JSON and have the entries associated with the action. This also checks
        that each entry is the expected datatype. Each purchase is tagged with a
        time index to differentiate timestamps.
        """
        try:
            entry = json.loads(log_str)
        except json.JSONDecodeError:
            self._log_invalid_entry(log_str)
            return
        if (all(k in entry for k in ('event_type', 'timestamp', 'id', 'amount')) and 
            entry['event_type'] == 'purchase'):
            try:
                entry['timestamp'] = datetime.strptime(entry['timestamp'], '%Y-%m-%d %H:%M:%S')
                entry['id'] = int(entry['id'])
                entry['amount'] = float(entry['amount'])
                entry['index'] = self._index
                self._index += 1
            except ValueError:
                self._log_invalid_entry(log_str)
                return
        elif (all(k in entry for k in ('event_type', 'timestamp', 'id1', 'id2')) and 
            entry['event_type'] in ('befriend', 'unfriend')):
            try:
                entry['timestamp'] = datetime.strptime(entry['timestamp'], '%Y-%m-%d %H:%M:%S')
                entry['id1'] = int(entry['id1'])
                entry['id2'] = int(entry['id2'])
            except ValueError:
                self._log_invalid_entry(log_str)
                return
        elif all(k in entry for k in ('D', 'T')):
            try:
                entry['D'] = int(entry['D'])
                entry['T'] = int(entry['T'])
            except ValueError:
                self._log_invalid_entry(log_str)
                return
        else:
            self._log_invalid_entry(log_str)
            return
        return entry
    
    
    def _load_network_data(self, entry):
        """Process a batch entry for initializing the friends and purchases.
        """
        if entry['event_type'] == 'purchase':
            purchase_info = (entry['index'], entry['timestamp'], entry['amount'])
            if entry['id'] in self.friends and entry['id'] in self.purchases:
                self.purchases[entry['id']].append(purchase_info)
            elif entry['id'] in self.friends:
                self.purchases[entry['id']] = [purchase_info]
            else:
                self.friends.add_node(entry['id'])
                self.purchases[entry['id']] = [purchase_info]
        elif entry['event_type'] == 'befriend':
            for c_id in (entry['id1'], entry['id2']):
                if c_id not in self.friends:
                    self.friends.add_node(c_id)
            self.friends.add_edge(entry['id1'], entry['id2'])
        elif entry['event_type'] == 'unfriend':
            for c_id in (entry['id1'], entry['id2']):
                if c_id not in self.friends:
                    self.friends.add_node(c_id)
            if self.friends.has_edge(entry['id1'], entry['id2']):
                self.friends.remove_edge(entry['id1'], entry['id2'])
        return
    
    def load_batch_log(self):
        """Loads initialization data into the class.
        
        Parameters
        ----------
        filename : str
            The path and name of the batch file.
        
        Returns
        -------
        None
        """
        with open(self.batch_file, 'r') as f:
            entry = self._validate_log_and_decode(f.readline())
            if not entry:
                raise ValueError('Parameters at top of batch file cannot be read.')
            self.d = entry['D']
            self.t = entry['T']
            for l in f:
                entry = self._validate_log_and_decode(l)
                if not entry:
                    continue
                self._load_network_data(entry)
        return
    
    def _get_neighbors(self, node, degree=1):
        """A recursive function for extracting a list of all the neighbors of
        degree `degree` for node `node`. This uses a private class attribute
        to keep track of all of the visited nodes. This is kind of like a BFS
        with recursion. Add the node in degree 1 case for consistency.
        """
        if degree == 1:
            self._seen.update(self.friends.neighbors(node) + [node])
            return
        elif degree > 1:
            neighbors = self.friends.neighbors(node)
            not_seen = set(neighbors) - self._seen
            if not_seen:
                degree = degree - 1
                for neighbor in not_seen:
                    self._seen = self._seen | not_seen
                    self._get_neighbors(neighbor, degree=degree)
            else:
                return
    
    def _run_get_neighbors(self, node, degree=1):
        """A wrapper function for `_get_neighbors`. This is used to reset the
        `_seen` variable, run the function itself, and transform the output.
        """
        self._seen = set([])
        self._get_neighbors(node, degree=degree)
        neighbors = list(self._seen - set([node]))
        return neighbors
    
    def _combine_purchases(self, neighbors, t=2):
        """Combine most recent `t` purchases of all neighbors found. Will only
        pick `t` items from each person because there's a max of `t` items. They
        are collected into a store, which is reverse-sorted in-place and the
        first `t` items are taken.
        """
        store = []
        for node in neighbors:
            node_buys = self.purchases[node]
            if len(node_buys) <= t:
                store.extend(node_buys)
            else:
                store.extend(node_buys[-t:])
        store.sort(key=lambda x: x[0], reverse=True)
        return store[:t]
    
    def _purchase_statistics(self, purchases):
        """Go through the list of purchases and calculate mean and standard
        deviation on the prices.
        """
        amounts = []
        for purchase in purchases:
            amounts.append(purchase[-1])
        return np.mean(amounts), np.std(amounts)
    
    def _flag_entry(self, entry, mean, std):
        """Log anomalous purchases by writing those entries, along with the
        mean and standard deviation used to flag them, into a log file.
        """
        entry = entry.copy()
        entry['timestamp'] = entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        flagged = OrderedDict()
        for field in ('event_type', 'timestamp', 'id', 'amount'):
            flagged[field] = str(entry[field])
        flagged['mean'] = '{0:.2f}'.format(mean)
        flagged['sd'] = '{0:.2f}'.format(std)
        with open(self.flag_file, 'a') as f:
            f.write(json.dumps(flagged) + '\n')
        return
    
    def _process_stream_entry(self, entry):
        """Process a JSON entry of the stream of purchases after the batch
        file initialization. This retrieves neighbors, combines their purchases.
        If purchases are less than 2, nothing happens (but the entry is added
        to the purchase data later). Otherwise, the statistics are calculated
        and if the amount is greater than 3 times the standard deviation of the
        average purchases in the person's network, then it's flagged.
        """
        if entry['event_type'] == 'purchase':
            neighbors = self._run_get_neighbors(entry['id'], degree=self.d)
            if not neighbors:
                return
            t_purchases = self._combine_purchases(neighbors, t=self.t)
            if len(t_purchases) < 2:
                return
            mean, std = self._purchase_statistics(t_purchases)
            if entry['amount'] > (mean + self.s * std):
                self._flag_entry(entry, mean, std)
        return
    
    def load_stream_log(self):
        """Loads the data stream of purchases.
        
        Parameters
        ----------
        filename : str
            The filename for the stream file.
        """
        with open(self.stream_file, 'r') as f:
            for l in f:
                entry = self._validate_log_and_decode(l)
                if not entry:
                    continue
                self._process_stream_entry(entry)
                self._load_network_data(entry)
        return


def main(args):
    open(args.flag_file, 'w').close()
    open(args.invalid_file, 'w').close()
    
    customers = CustomerNetwork(batch_file=args.batch_file, stream_file=args.stream_file, flag_file=args.flag_file, invalid_file=args.invalid_file, s=args.std_threshold)
    customers.load_batch_log()
    customers.load_stream_log()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script builds a social network of friends and their purchases from batch data and then looks at stream data to flag purchases that are unusually high.')
    
    parser.add_argument('--batch-file', 
        metavar='<filename>', 
        help='filename for the input batch file. i.e. ../log_input/batch_log.json', 
        required=True)
    
    parser.add_argument('--stream-file', 
        metavar='<filename>', 
        help='filename for the input stream file. i.e. ../log_input/stream_log.json', 
        required=True)
    
    parser.add_argument('--flag-file', 
        metavar='<filename>', 
        help='filename for the output flagged file. i.e. ../log_output/flagged_purchases.json', 
        required=True)
    
    parser.add_argument('--invalid-file', 
        metavar='<filename>', 
        help='filename for the invalid entries file. i.e. ../log_output/invalid_entries.txt', 
        required=True)
    
    parser.add_argument('--std-threshold', 
        metavar='<int>', 
        help='The number of standard deviations to be above the mean for flagging. (default=3)', 
        required=False)    
    
    args = parser.parse_args()
    main(args)

        


