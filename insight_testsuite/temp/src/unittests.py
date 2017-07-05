"""
Tests
-----
Test the private methods of `CustomerNetwork` for test-driven development.
"""

import sys
SRC_PATH = '../src/'
sys.path.append(SRC_PATH)

import unittest
import networkx as nx
from datetime import datetime
from flagged_purchases import CustomerNetwork


class TestCustomerNetwork(unittest.TestCase):
    
    test_network = CustomerNetwork()
    
    test_graph = nx.Graph()
    test_graph.add_nodes_from(range(8))
    test_graph.add_edges_from([(0, 1), (1, 6), (0, 3), (0, 4), (3, 4), (4, 4), (3, 5), (6, 7)])
    
    def test_purchase_entry_decoding(self):
        test_entry_str1 = '{"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "16.83"}'
        test_entry_pass1 = self.__class__.test_network._validate_log_and_decode(test_entry_str1)
        self.assertEqual(test_entry_pass1, {'event_type':'purchase', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id': 1, 'amount': 16.83, 'index': 0})
    
    def test_befriend_entry_decoding(self):
        test_entry_str2 = '{"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "2"}'
        test_entry_pass2 = self.__class__.test_network._validate_log_and_decode(test_entry_str2)
        self.assertEqual(test_entry_pass2, {'event_type':'befriend', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id1': 1, 'id2': 2})
    
    def test_unfriend_entry_decoding(self):
        test_entry_str3 = '{"event_type":"unfriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "3"}'
        test_entry_pass3 = self.__class__.test_network._validate_log_and_decode(test_entry_str3)
        self.assertEqual(test_entry_pass3, {'event_type':'unfriend', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id1': 1, 'id2': 3})
    
    def test_batch_entry(self):
        self.__class__.test_network._load_network_data({'event_type':'purchase', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id': 1, 'amount': 16.83, 'index': 0})
        self.__class__.test_network._load_network_data({'event_type':'befriend', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id1': 1, 'id2': 2})
        self.__class__.test_network._load_network_data({'event_type':'unfriend', 'timestamp': datetime(2017, 6, 13, 11, 33, 1), 'id1': 1, 'id2': 3})
        self.assertEqual(self.__class__.test_network.friends.nodes(), [1, 2, 3])
        self.assertEqual(self.__class__.test_network.friends.edges(), [(1, 2)])
        self.assertEqual(self.__class__.test_network.purchases[1], [(0, datetime(2017, 6, 13, 11, 33, 1), 16.83)])
    
    def test_get_neighbors_degree_1(self):
        self.__class__.test_network.friends = self.__class__.test_graph
        self.__class__.test_network._get_neighbors(0, degree=1)
        self.assertEqual(self.__class__.test_network._seen, set([0, 1, 3, 4]))
    
    def test_get_neighbors_degree_2(self):
        self.__class__.test_network.friends = self.__class__.test_graph
        self.__class__.test_network._seen = set([])
        self.__class__.test_network._get_neighbors(0, degree=2)
        self.assertEqual(self.__class__.test_network._seen, set([0, 1, 3, 4, 5, 6]))
    
    def test_get_neighbors_none_seen(self):
        self.__class__.test_network.friends = self.__class__.test_graph
        self.__class__.test_network._seen = set([])
        self.__class__.test_network._get_neighbors(2, degree=2)
        self.assertEqual(self.__class__.test_network._seen, set([]))

    def test_combine_purchases_and_statistics(self):
        self.__class__.test_network.friends = self.__class__.test_graph
        test_purchases = {
            0: [
                (0, datetime(2017, 6, 13, 11, 33, 1), 16.83), 
                (2, datetime(2017, 6, 13, 11, 33, 1), 59.28),
               ],
            4: [
                (1, datetime(2017, 6, 13, 11, 33, 1), 11.20), 
               ],  
            }
        self.__class__.test_network.purchases = test_purchases
        test_total = self.__class__.test_network._combine_purchases([0, 4], t=2)
        self.assertEqual(test_total, [(2, datetime(2017, 6, 13, 11, 33, 1), 59.28), (1, datetime(2017, 6, 13, 11, 33, 1), 11.20)])
        self.assertEqual('{0:.2f} {1:.2f}'.format(*self.__class__.test_network._purchase_statistics(test_total)), '35.24 24.04')


if __name__ == '__main__':
    unittest.main()