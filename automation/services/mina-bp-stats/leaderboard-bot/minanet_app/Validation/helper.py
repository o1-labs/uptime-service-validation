import networkx as nx
import pandas as pd

#I'm duplicating the code here at the moment, until we decide what direction to take.
#I've explicitly set some of the base variables.

def filter_state_hash_percentage(df, p=0.34):
    state_hash_list = df['state_hash'].value_counts().sort_values(ascending=False).index.to_list()
    # get 34% number of blk in given batch
    total_unique_blk = df['block_producer_key'].nunique()
    percentage_result = round(total_unique_blk * p, 2)
    good_state_hash_list = list()
    for s in state_hash_list:
        blk_count = df[df['state_hash'] == s]['block_producer_key'].nunique()
        # check blk_count for state_hash submitted by blk least 34%
        if blk_count >= percentage_result:
            good_state_hash_list.append(s)
    return good_state_hash_list


def create_graph(batch_df, p_selected_node_df, c_selected_node, p_map):
    batch_graph = nx.DiGraph()
    parent_hash_list = batch_df['parent_state_hash'].unique()
    state_hash_list = set(list(batch_df['state_hash'].unique()) + list(p_selected_node_df['state_hash'].values))
    selected_parent = [parent for parent in parent_hash_list if parent in state_hash_list]
    """ t1=[w[42:] for w in list(p_selected_node_df['state_hash'].values)]
    t2=[w[42:] for w in c_selected_node]
    t3=[w[42:] for w in state_hash_list]
    batch_graph.add_nodes_from(t1)
    batch_graph.add_nodes_from( t2)
    batch_graph.add_nodes_from(t3) """

    batch_graph.add_nodes_from(list(p_selected_node_df['state_hash'].values))
    batch_graph.add_nodes_from(c_selected_node)
    batch_graph.add_nodes_from(state_hash_list)

    for row in batch_df.itertuples():
        state_hash = getattr(row, 'state_hash')
        parent_hash = getattr(row, 'parent_state_hash')

        if parent_hash in selected_parent:
            batch_graph.add_edge(parent_hash, state_hash)

    #  add edges from previous batch nodes
    batch_graph.add_edges_from(p_map)

    return batch_graph

def apply_weights(batch_graph, c_selected_node, p_selected_node):
    for node in list(batch_graph.nodes()):
        if node in c_selected_node:
            batch_graph.nodes[node]['weight'] = 0
        elif node in p_selected_node['state_hash'].values:
            batch_graph.nodes[node]['weight'] = p_selected_node[p_selected_node['state_hash'] == node]['weight'].values[
                0]
        else:
            batch_graph.nodes[node]['weight'] = 9999

    g_pos = None  # plot_graph(batch_graph, None, '1. first apply')
    return batch_graph, g_pos

def bfs(graph, queue_list, node, batch_statehash, g_pos):
    visited = list()
    visited.append(node)
    cnt = 2
    while queue_list:
        m = queue_list.pop(0)
        for neighbour in list(graph.neighbors(m)):
            if neighbour not in visited:
                graph.nodes[neighbour]['weight'] = get_minimum_weight(graph, neighbour)
                visited.append(neighbour)
                # if not neighbour in visited:
                queue_list.append(neighbour)
        # plot_graph(graph, g_pos, str(cnt)+'.'+m)
        cnt += 1
    shortlisted_state = []
    hash_weights = []
    for node in list(graph.nodes()):
        if graph.nodes[node]['weight'] <= 2:
            # if node in batch_statehash:
            shortlisted_state.append(node)
            hash_weights.append(graph.nodes[node]['weight'])

    shortlisted_state_hash_df = pd.DataFrame()
    shortlisted_state_hash_df['state_hash'] = shortlisted_state
    shortlisted_state_hash_df['weight'] = hash_weights
    return shortlisted_state_hash_df

def get_minimum_weight(graph, child_node):
    child_node_weight = graph.nodes[child_node]['weight']
    for parent in list(graph.predecessors(child_node)):
        lower = min(graph.nodes[parent]['weight'] + 1, child_node_weight)
        child_node_weight = lower
    return child_node_weight