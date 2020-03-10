import os
import re

import psycopg2
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt



def draw_dynamic_networkx_labels(
        G: nx.Graph,
        pos,
        font_size_factor=1,
        font_color='k',
        font_family='sans-serif',
        font_weight='normal',
        alpha=None,
        bbox=None,
        ax=None,
        **kwds):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        raise ImportError("Matplotlib required for draw()")
    except RuntimeError:
        print("Matplotlib unable to open display")
        raise
    weights = nx.get_node_attributes(G, 'weight')

    if ax is None:
        ax = plt.gca()

    # set optional alignment
    horizontalalignment = kwds.get('horizontalalignment', 'center')
    verticalalignment = kwds.get('verticalalignment', 'center')

    text_items = {}  # there is no text collection so we'll fake one
    for node in G.nodes():
        (x, y) = pos[node]
        label = str(node)
        t = ax.text(
            x, y,
            label,
            size=font_size_factor * weights[node],
            color=font_color,
            family=font_family,
            weight=font_weight,
            alpha=alpha,
            horizontalalignment=horizontalalignment,
            verticalalignment=verticalalignment,
            transform=ax.transData,
            bbox=bbox,
            clip_on=True)
        text_items[node] = t

    ax.tick_params(
        axis='both',
        which='both',
        bottom=False,
        left=False,
        labelbottom=False,
        labelleft=False)

    return text_items



conn = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'],
    dbname='barberini',
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'])
cur = conn.cursor()
cur.execute('select text from google_maps_review')
reviews = cur.fetchall()
#reviews = reviews[:100]  # debug
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

pos = nx.spring_layout(G)
nx.draw_networkx_nodes(G, pos, node_size=0.03)
nx.draw_networkx_edges(G, pos, width=0.01, alpha=0.1)
draw_dynamic_networkx_labels(G, pos, font_size_factor=1000)
plt.savefig("graphs/reviews.png", dpi=1000)