# %%
import json
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_scripts.sql_generators import create_deduplication_sql, truncate_tables
# %%
filename = "arxiv-metadata-oai-snapshot.json"
data = []
with open(filename, 'r') as f:
    i = 0
    for line in f:
        data.append(line)
        i += 1
        if i > 250:
            break
with open('data_sample_250.json', 'w') as f:
    for line in data:
        f.write(line)

# %%
print(create_deduplication_sql('staging', 'person', ['first_name', 'last_name', 'third_name'], 
                               'authorship','author_id'))
# %%
tables = ['journal', 'version', 'license', 'publication', 
          'publication_journal', 'person', 'authorship',
          'sub_category', 'category', 'publication_category']
print(truncate_tables('dwh', tables))
# %%
print(truncate_tables('staging', tables))
# %%
