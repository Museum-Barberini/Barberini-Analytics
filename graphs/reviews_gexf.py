import os
import re
from collections import OrderedDict
from random import random

import psycopg2
import pandas as pd
import networkx as nx
from colour import Color


conn = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'],
    dbname='barberini',
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'])
cur = conn.cursor()
cur.execute('select text from google_maps_review')
reviews = cur.fetchall()
reviews = reviews[:100]  # debug
texts = [re.sub('[;\r\n]', '', text) for [text] in reviews if text]
text_words = {text: [word.lower() for word in text.split(' ')] for text in texts}

#ignored = set([''] + list('-+/&') + [word.lower() for word in pd.read_csv('ignored_words.csv', header=None)[0]])
words = [word for words in text_words.values() for word in words]

#cleansed_words = [word for word in words if word not in ignored]

word_counts = pd.Series(words).value_counts()

G = nx.Graph()
for word, count in word_counts.iteritems():
    G.add_node(word, weight=float(count)/len(words))
for text, words in text_words.items():
    edge_weights = {}
    for word1 in words:
        for word2 in words:
            if word1 >= word2: continue
            if (word1, word2) not in edge_weights:
                edge_weights[(word1, word2)] = 0
            edge_weights[(word1, word2)] += 1
    G.add_weighted_edges_from([list(edge) + [weight] for edge, weight in edge_weights.items()])

#nx.write_gexf(G, 'output/reviews.gexf')

DEBUG_LIMIT = 90 # sometimes works, sometimes fails!
G.add_node(str(DEBUG_LIMIT), weight=0.05)
node_weights = nx.get_node_attributes(G, 'weight')
def gexf_escape(str):
    return str.replace('\'', '\\')
nodes = list(G.nodes)
node_ids = {item: index for index, item in enumerate(nodes)}
nodes.sort(key=lambda node: node_weights[node], reverse=True)
### DEBUG
nodes = nodes[:DEBUG_LIMIT]
nodes = list([node for node in nodes if node])
G = G.subgraph(nodes)
### BUGED
edges = list(G.edges)
edge_ids = {item: index for index, item in enumerate(edges)}
edges.sort(key=lambda edge: G[edge[0]][edge[1]]['weight'], reverse=True)
edges = edges[:50]

gexf_contents = f"""
<?xml version="1.0" encoding="UTF-8"?>
<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2" 
	xmlns:viz="http://www.gexf.net/1.2draft/viz" 
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd">
	<graph defaultedgetype="directed" timeformat="double" mode="dynamic">
        <nodes>
        {'''
        '''.join([
            f'''
            <node id="{node_ids[node]}" label="{gexf_escape(node)}">
                <viz:size value="{node_weights[node] * 100 + random() / 10}"></viz:size>
                <viz:color r="{(node_weights[node] - node_weights[node] + 0.05) * 4000}" g="12" b="12"></viz:color>
            </node>
            '''
            for node in nodes
        ])}
        </nodes>
		<edges>
        {'''
        '''.join([
            f'''<edge
                id="{edge_ids[node1, node2]}"
                source="{node_ids[node1]}" target="{node_ids[node2]}"
                weight="{G[node1][node2]['weight'] + random() / 10}" />
            '''
            for node1, node2 in edges
        ])}
        </edges>
    </graph>
</gexf>
"""
with open('output/reviews.gexf', 'w', encoding='utf-8') as file:
    file.write(gexf_contents)
