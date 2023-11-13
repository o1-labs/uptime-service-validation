import unittest
import numpy as np
from helper import filter_state_hash_percentage, create_graph, apply_weights, bfs
import pandas as pd

class TestingGraphMethods(unittest.TestCase):
    def test_filter_state_hash_single(self):
        master_state_hash = pd.DataFrame([['state_hash_1', 'block_producer_key_1']], 
                                         columns=['state_hash', 'block_producer_key'])
        output = filter_state_hash_percentage(master_state_hash)
        self.assertEqual(output, ['state_hash_1'])

    def test_filter_state_hash_multi(self):
        master_state_hash = pd.DataFrame([['state_hash_1', 'block_producer_key_1'], 
                                          ['state_hash_1', 'block_producer_key_2'], 
                                          ['state_hash_2', 'block_producer_key_3']], 
                                          columns=['state_hash', 'block_producer_key'])
        output = filter_state_hash_percentage(master_state_hash)
        self.assertEqual(output, ['state_hash_1'])

# The create_graph function creates a graph and adds all the state_hashes that appear in the batch as nodes, as well as the hashes from the previous batch.
# It also adds edges between any child and parent hash (this is even for parent-child relationship between batches)
# The arguments are:
# --batch_df: state-hashes of current batch.
# --p_selected_node_df: these are all the (short-listed) state-hashes from the previous batch (as well as their weights).
# --c_selected_node: these are the hashes from the current batch above 34% threshold
# --p_map: this lists the parent-child relationships in the previous batch.

    def test_create_graph_count_number_of_nodes_and_edges(self):
        # current batch that was downloaded
        batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], ['state_hash_2', 'parent_state_hash_2']], columns=['state_hash', 'parent_state_hash'])
        # previous_state_hashes with weight
        p_selected_node_df = pd.DataFrame(['parent_state_hash_1'], columns=['state_hash'])
        # filtered_state_hashes  
        c_selected_node =  ['state_hash_1', 'state_hash_2']
        # relations between parents and children, i.e. those previous stte hashes that are parents in this batch.
        p_map = [['parent_state_hash_1', 'state_hash_1']]
        output = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        # total number of nodes is always those in the current batch + those from previous
        self.assertEqual(len(output.nodes), len(batch_df) + len(p_selected_node_df))
        # there are no nodes in the current batch that are also parents of later nodes in the batch (see next test)
        self.assertEqual(len(output.edges), len(p_map))

    def test_create_graph_count_number_of_nodes_and_edges_nested(self):
        # current batch that was downloaded
        batch_df = pd.DataFrame([
            ['state_hash_1', 'parent_state_hash_1'], 
            ['state_hash_2', 'state_hash_1'], 
            ['state_hash_3', 'state_hash_2']], 
            columns=['state_hash', 'parent_state_hash'])
        # previous_state_hashes with weight
        p_selected_node_df = pd.DataFrame([['parent_state_hash_1'] ,['parent_state_hash_2']], columns=['state_hash'])
        # filtered_state_hashes  
        c_selected_node =  ['state_hash_1', 'state_hash_2']
        # relations between parents and children, i.e. those previous stte hashes that are parents in this batch.
        p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
        output = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        # total number of nodes is the same
        self.assertEqual(len(output.nodes), len(batch_df) + len(p_selected_node_df))
        # total number of edges is the parent-child relations in p_map, but plus also the parent-child relationships in the batch (i.e. 2) and between the two batches (i.e. 1).      
        self.assertEqual(len(output.edges), len(p_map) + 3)

# The apply_weights function sets the weights to 0 for any node above the 34% threshold and if a parent_hash to the weight computed form last time.
# The arguments are:
# --batch_df: state-hashes of current batch.
# --p_selected_node_df: these are all the (short-listed) state-hashes from the previous batch (as well as their weights).
# --c_selected_node: these are the hashes from the current batch above 34% threshold
    def test_apply_weights_sum_weights_empty_parents_and_empty_selected_node(self):
        batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], 
                                 ['state_hash_2', 'state_hash_1'], 
                                 ['state_hash_3', 'state_hash_2']], 
                                 columns=['state_hash', 'parent_state_hash'])
        p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123],
                                           ['parent_state_hash_2', 345]], 
                                           columns=['state_hash', 'weight'])
        c_selected_node =  ['state_hash_1', 'state_hash_2']
        p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
        batch_graph = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        # pass in empty short-lists and parent nodes to the weight function and ensure every node has infinite weighting.
        c_selected_node_empty = []
        p_selected_node_df_empty = pd.DataFrame([], columns=['state_hash', 'weight'])
        weighted_graph, g_nod = apply_weights(batch_graph, c_selected_node_empty, p_selected_node_df_empty)
        self.assertEqual(len(list(weighted_graph.nodes)), 5)
        for node in list(weighted_graph.nodes):
            self.assertEqual(weighted_graph.nodes[node]['weight'], 9999)

    def test_apply_weights_sum_weights_nested(self):
        batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], 
                                 ['state_hash_2', 'state_hash_1'], 
                                 ['state_hash_3', 'state_hash_2']], 
                                 columns=['state_hash', 'parent_state_hash'])
        p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123], 
                                           ['parent_state_hash_2', 345]], 
                                           columns=['state_hash', 'weight'])
        c_selected_node =  ['state_hash_1', 'state_hash_2']
        p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
        batch_graph = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        weighted_graph, g_nod = apply_weights(batch_graph, c_selected_node, p_selected_node_df)
        self.assertEqual(len(list(weighted_graph.nodes)), 5)
        for node in list(weighted_graph.nodes):
            if node == 'state_hash_1':
                self.assertEqual(weighted_graph.nodes[node]['weight'], 0)
            if node == 'state_hash_2':
                self.assertEqual(weighted_graph.nodes[node]['weight'], 0)
            if node == 'state_hash_3':
                self.assertEqual(weighted_graph.nodes[node]['weight'], 9999)
            if node == 'parent_state_hash_1':
                self.assertEqual(weighted_graph.nodes[node]['weight'], 123)
            if node == 'parent_state_hash_2':
                self.assertEqual(weighted_graph.nodes[node]['weight'], 345)            
    
# The bfs is what computes the weight for nodes that aren't previous hashes or above the 34% threshold (which automatically have weight 0).
# The bfs output actually includes the parent-hashes, as well, and all those hashes from the current batch with computed weight <= 2.
# The arguments are:
# --graph: weighted graph computed from create_graph and apply_weights function
# --queue_list: these are the parent_hashes and the theshold hashes from the current batch.
# --node: first element of the queue
# --batch_statehash: presumably all the state-hashes from the current batch and is unused (could be removed)
# --g_pos: this is always None and is unused (could be removed)
    def test_bfs_easy(self):
        batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'],
                                ['state_hash_2', 'state_hash_1'], 
                                ['state_hash_3', 'state_hash_2']], 
                                columns=['state_hash', 'parent_state_hash'])
        p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123],
                                           ['parent_state_hash_2', 345]], 
                                           columns=['state_hash', 'weight'])
        # empty short-list
        c_selected_node =  ['state_hash_1', 'state_hash_2']
        p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
        batch_graph = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        weighted_graph, g_nod = apply_weights(batch_graph, c_selected_node, p_selected_node_df)        
        queue_list = list(p_selected_node_df['state_hash'].values) + c_selected_node
        batch_state_hash = list(batch_df['state_hash'].unique())
        shortlist = bfs(graph=weighted_graph, queue_list=queue_list, node=queue_list[0],
                                                    batch_statehash=batch_state_hash, g_pos=None)
        expected = pd.DataFrame([['state_hash_1', 0], ['state_hash_2', 0], ['state_hash_3', 1]], columns=['state_hash','weight'])
        pd.testing.assert_frame_equal(shortlist, expected)

    def test_bfs_hard(self):
        batch_df = pd.DataFrame([
            ['state_hash_11', 'parent_state_hash_1'], 
            ['state_hash_12', 'state_hash_11'], 
            ['state_hash_13', 'state_hash_12'], 
            ['state_hash_14', 'state_hash_13'],
            ['state_hash_15', 'state_hash_14'],
            ['state_hash_16', 'state_hash_15'],
            ['state_hash_21', 'parent_state_hash_2'], 
            ['state_hash_22', 'state_hash_21'], 
            ['state_hash_23', 'state_hash_22'], 
            ['state_hash_24', 'state_hash_23'],
            ['state_hash_25', 'state_hash_24'],
            ['state_hash_26', 'state_hash_25']
            ], 
            columns=['state_hash', 'parent_state_hash'])
        p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 1], 
                                           ['parent_state_hash_2', 1]], 
                                           columns=['state_hash', 'weight'])
        c_selected_node =  ['state_hash_11', 'state_hash_21']
        p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
        batch_graph = create_graph(batch_df, p_selected_node_df, c_selected_node, p_map)
        weighted_graph, g_nod = apply_weights(batch_graph, c_selected_node, p_selected_node_df)        
        queue_list = list(p_selected_node_df['state_hash'].values) + c_selected_node
        batch_state_hash = list(batch_df['state_hash'].unique())
        shortlist = bfs(weighted_graph, queue_list, queue_list[0], batch_state_hash, g_pos=None)
        expected = pd.DataFrame([['parent_state_hash_1', 1],
                                 ['parent_state_hash_2', 1],
                                 ['state_hash_11', 0],
                                 ['state_hash_21', 0], 
                                 ['state_hash_12', 1],
                                 ['state_hash_22', 1], 
                                 ['state_hash_13', 2],
                                 ['state_hash_23', 2]], 
                                 columns=['state_hash','weight'])
        self.assertEqual(set(shortlist['state_hash']), set(expected['state_hash']))

if __name__ == '__main__':
    unittest.main()