# %%
import json
from glob import glob
import pandas as pd
# %%
filename = "data/arxiv-metadata-full.json"
with open(filename, 'rb') as f:
    i = 0
    for line in f:
        # Process each line
        json_data = json.loads(line.strip())
        print(json_data)
        i+= 1
# %%
for path in glob('data/raw_data/*'):
    data = []
    with open(path, 'r') as f: 
        for line in f:
            data.append(json.loads(line.strip()))
    df = pd.DataFrame(data)
    print(df)
# %%
