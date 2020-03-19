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
all_reviews = {}
sources = ['appstore_review', 'google_maps_review', 'tweet']
for source in sources:
    cur.execute(f'select text from {source}')
    all_reviews[source] = cur.fetchall()
all_texts = {
    source: [
        re.sub('[;\r\n,;]', '', text).lower()
        for [text] in reviews
        if text
    ]
    for source, reviews in all_reviews.items()
}
stopwords = set([''] + list('-–—+/&.,;:') + [word.lower() for word in pd.read_csv('graphs/stopwords.csv', header=None)[0]])
stopwords.add('museumbarberini')
all_text_words = {
    source: {
        text: [
            word
            for word in [word.strip(',-–—+/&.,;:!?#@_()[]"\'') for word in text.split(' ')]
            if word not in stopwords]
        for text in texts
    }
    for source, texts in all_texts.items()
}
"""
further cleansing ideas:
- define custom ignore words/replacements (e. g., museumbarberini -> barberini)
"""

all_words = {
    source: [
        word
        for words in text_words.values()
        for word in words
    ]
    for source, text_words in all_text_words.items()
}

word_counts = {
    source: pd.Series(words).value_counts()
    for source, words in all_words.items()
}
all_word_counts = {}
for source, word_count in word_counts.items():
    for word, count in word_count.iteritems():
        if word not in all_word_counts:
            all_word_counts[word] = {source: 0 for source in sources}
        word_counts = all_word_counts[word]
        word_counts[source] = float(count)/len(all_words[source]) # todo

G = nx.Graph()
for word, counts in all_word_counts.items():
    G.add_node(word, weights=counts)
for source, text_words in all_text_words.items():
    edge_weights = {}
    for text, words in text_words.items():
        for index1, word1 in enumerate(words):
            for index2, word2 in enumerate(words):
                if index1 >= index2: continue  # don't match twice
                if (word1, word2) not in edge_weights:
                    edge_weights[(word1, word2)] = 0
                edge_weights[(word1, word2)] += index2 - index1
    G.add_weighted_edges_from([list(edge) + [weight] for edge, weight in edge_weights.items()])

#nx.write_gexf(G, 'output/reviews.gexf')

DEBUG_LIMIT = 200  # sometimes works, sometimes fails!
#G.add_node(str(DEBUG_LIMIT), weight=0.05)
all_node_weights = nx.get_node_attributes(G, 'weights')
node_weights = {node: sum(weights.values()) for node, weights in all_node_weights.items()}
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
#edges = edges[:50]

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
                <viz:size value="{node_weights[node] * len(words) + random() / 10000}"></viz:size>
                <viz:color r="{round(all_node_weights[node][sources[0]] * 20000)}" g="{round(all_node_weights[node][sources[1]] * 20000)}" b="{round(all_node_weights[node][sources[2]] * 20000)}"></viz:color>
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
