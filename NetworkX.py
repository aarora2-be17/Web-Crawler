import networkx as nx
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

db = sqlite3.connect('spider5.sqlite')
cur = db.cursor()

df = pd.read_sql_query('SELECT * FROM LINKS', db)

G = nx.from_pandas_edgelist(df, 'from_id', 'to_id' )

plt.figure(figsize=(10,9))

node_color = [G.degree(v) for v in G]
nx.draw_networkx(G, node_color=node_color, alpha=0.7, with_labels=False, edge_color='.4', cmap=plt.cm.Blues)

plt.axis('off')
#plt.tight_layout()

pr = nx.pagerank(G, alpha=0.9)
print(pr)
for k,v in pr.items():
    cur.execute('UPDATE Pages SET page_rank = ( ? ) WHERE id = ( ? )', (v, k))

df = pd.read_sql_query('SELECT * FROM Pages', db)
print(df.head())