import json
from datetime import datetime
from copy import deepcopy
from glob import glob
from pathlib import Path
import shutil
import uuid

PROGRESS_FILE_PATH = '/tmp/data/progress.json'

def load_progress():
    try:
        if os.path.exists(PROGRESS_FILE_PATH):
            with open(PROGRESS_FILE_PATH, 'r') as file:
                return json.load(file)
    except json.JSONDecodeError:
        return {}
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE_PATH, 'w') as file:
        json.dump(progress, file)

def process_categories(publication_data):
    categories_str = publication_data['categories']
    categories_split = categories_str.split(' ')
    category_data = []
    for category_str in categories_split:
        if '.' in category_str:
            category_name = category_str.split('.')[0]
            subcategory_name = category_str.split('.')[1]
        else: 
            category_name = categories_str
            subcategory_name = None
        category_data.append({'category_name': category_name,
                              'subcategory_name': subcategory_name})
    #publication_data['categories_parsed'] = category_data
    return category_data

def process_authors(publication_data):
    author_objs = []
    author_lists = publication_data['authors_parsed']
    for author_list in author_lists:
        first_name = author_list[1]
        last_name = author_list[0]
        third_name = author_list[2]
        author_obj = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'third_name': third_name
                    }
        author_objs.append(author_obj)
    return author_objs
        
def process_submitter(publication_data):
    submitter_str = publication_data['submitter']
    split_str = submitter_str.split(' ')
    first_name, last_name, third_name = None, None, None
    first_name = split_str[0]
    if len(split_str) > 1:
        last_name = split_str[1]
    if len(split_str) > 2:
        third_name = split_str[2]
    submitter_obj = {
        'first_name': first_name,
        'last_name': last_name,
        'third_name': third_name
    }
    return submitter_obj

def process_versions(publication_data):
    versions = publication_data['versions']
    versions_norm = []
    for version in versions:
        version_norm = {'version_no': int(version['version'].split('v')[1]),
                        'create_date': datetime.strptime(version['created'], 
                                                         "%a, %d %b %Y %H:%M:%S GMT").isoformat()}
        versions_norm.append(version_norm)
    return versions_norm
    

def preprocess_publication(publication_data):
    data_proc = deepcopy(publication_data)
    data_proc['norm_authors'] = process_authors(publication_data)
    data_proc['norm_categories'] = process_categories(publication_data)
    data_proc['arxiv_id'] = data_proc['id']
    data_proc['submitter_norm'] = process_submitter(publication_data)
    data_proc['versions_norm'] = process_versions(publication_data)

    del data_proc['abstract'] 
    del data_proc['report-no']
    del data_proc['authors']
    del data_proc['authors_parsed']
    del data_proc['categories']
    del data_proc['id']
    del data_proc['submitter']
    del data_proc['versions']

    return data_proc


def process_file(path, output_path, failed_lines, norm_datas, progress, batch_size, n_batches):
    n_lines = progress.get(path, 0)
    with open(path, 'rb') as f:
        for line_number, line in enumerate(f):
            if line_number < n_lines:
                continue
            try:
                publication_data = json.loads(line.strip())
            except:
                failed_lines.append(line)
                continue                    
            try:
                norm_data = preprocess_publication(publication_data)
                norm_datas.append(norm_data)
            except:
                failed_lines.append(line)
                continue
            
            n_lines += 1
            if n_lines % batch_size == 0:
                # Not handling exceptions here as it's perferrable to crash as errors here are not related to input data
                with open(f'{output_path}/{uuid.uuid1()}.json', 'w') as f:
                    json.dump(norm_datas, f, indent=4)
                norm_datas = []
                n_batches -= 1

            if n_batches == 0:
                progress[path] = n_lines
                save_progress(progress)
                break
    return norm_datas

def load_and_preprocess(data_path, output_path, success_path, fail_path, batch_size, n_batches):
    norm_datas = []
    progress = load_progress()
    print(progress)
    for path in glob(data_path):
        failed_lines = []
        try:
            norm_datas = process_file(path, output_path, failed_lines, norm_datas, progress, batch_size, n_batches)
        except:
            shutil.move(path, Path(fail_path)/Path(path).name)

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(failed_lines) > 0:
            with open(Path(fail_path)/f'{Path(path).name}_{current_time}_failed_lines.txt', 'w') as file:
                for item in failed_lines:
                    file.write("%s" % item)

    if len(norm_datas) > 0:
        with open(f'{output_path}/{uuid.uuid1()}.json', 'w') as f:
            json.dump(norm_datas, f, indent=4)
        shutil.move(path, Path(success_path)/Path(path).name)