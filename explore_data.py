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
        if i > 2000:
            break
with open('data_sample_2000.json', 'w') as f:
    for line in data:
        f.write(line)
# %%
with open('data_sample_250.json', 'r') as f:
       for line in f:
            data = json.loads(line)
            categories_str = data['categories']
            categories_split = categories_str.split(' ')
            category_data = []
            for category_str in categories_split:
                if '.' in category_str:
                    category_name = category_str.split('.')[0]
                    subcategory_name = category_str.split('.')[1]
                else: 
                    category_name = category_str
                    subcategory_name = None
                if ' ' in catego
                category_data.append({'category_name': category_name,
                                    'subcategory_name': subcategory_name})
                
            print(category_data)
        
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
"quant-ph  cs.IT math.IT".split(' ')
# %%
print("math.QA math-ph math.MP"[7])
# %%
