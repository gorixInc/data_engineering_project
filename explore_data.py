# %%
import json
from glob import glob
import pandas as pd
# %%
filename = "arxiv-metadata-full.json"
data = []
with open(filename, 'rb') as f:
    i = 0
    for line in f:
        # Process each line
        json_data = json.loads(line.strip())
        data.append(json_data)
        i+= 1

df = pd.DataFrame(data)
df.to_csv('full_data.csv')
# %%
df.columns
# %%
df = pd.read_csv('data/level1/sample_data.csv')
df = df.drop(['abstract'], axis=1)
# %%
df['journal-ref'].unique()
# %%
