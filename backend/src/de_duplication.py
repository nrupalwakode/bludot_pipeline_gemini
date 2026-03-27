import os
import numpy as np
import pandas as pd
import warnings
import pickle
import string
import pandas_dedupe
import re
import recordlinkage
from fuzzywuzzy import fuzz
from src.bludot_concat import date_formatting

warnings.filterwarnings("ignore")

def string_filter1(strings):
    # Construct the file path for the pickle file
    pkl_file_loc = os.path.join(os.getcwd(), 'src', 'updated_pickle.pkl')
    
    # Load the contents of the pickle file into a dictionary
    abbreviation_dict = pickle.load(open(r'{}'.format(pkl_file_loc), "rb"))
    
    # Convert the input string to lowercase and remove punctuation
    strings = strings.lower().translate(str.maketrans('', '', string.punctuation))
    
    # Split the string into a list of words
    strings = strings.split()
    
    # Iterate over each word in the list
    for i in range(len(strings)):
        word = strings[i]
        
        # Check if the word exists as a key in the abbreviation dictionary
        if word in abbreviation_dict.keys():
            # Replace the word with its corresponding value from the dictionary
            strings[i] = abbreviation_dict[word]
    
    # Join the modified list of words into a single string with spaces in between
    strings = ' '.join(strings)
    
    # Remove leading and trailing whitespace from the resulting string
    strings = strings.strip()
    
    # Return the final processed string
    return strings


def deduplications_of_city_records(city_data, pivot_table_summary, th):
    city_data = city_data.astype(str)
    city_data.fillna('', inplace=True)
    datalist = [[] for f in city_data]
    # print(len(datalist))
    for concat_name in city_data['cluster id'].value_counts().index:
        dataset = city_data[city_data['cluster id'] == concat_name]
        for column_name, list_number in zip(dataset.columns, range(0, len(datalist))):
            if column_name in pivot_table_summary[pivot_table_summary['Counts'] >= th].Columns_name.values:
                datalist[list_number].append(
                    dataset[f'{column_name}'].values)  # unique values
            else:
                datalist[list_number].append(
                    dataset[f'{column_name}'].values[0])

    selected_index_value = [datalist[index_values]
                            for index_values in range(0, len(datalist))]
    create_table = pd.DataFrame(selected_index_value)
    create_table = create_table.T
    create_table.columns = city_data.columns

    columns_details = []
    for column_name in create_table.columns:
        updated_table = pd.DataFrame(create_table[f'{column_name}'].tolist())
        updated_table.columns = [f'{column_name}_{column_count}' for column_count in range(
            1, updated_table.shape[1]+1)]
        columns_details.append(updated_table)

    deduplicated_table = pd.concat(columns_details, axis=1)

    return create_table, deduplicated_table

def fuzzy_deduplication(city_data,filename,th=None):
    city_data.fillna('',inplace=True)
    concat_detail_value_count = city_data['cluster id'].value_counts()
    duplicated_values = concat_detail_value_count[concat_detail_value_count > 1]
    unique_values = concat_detail_value_count[concat_detail_value_count == 1]

    print("*************************Pivot Table Summary********************")
    print(f"Total Number of Rows  :{city_data.shape}")
    print(f"Count greater than one:{duplicated_values.sum()}")
    print(f"Count equal to one    :{unique_values.sum()}")
    print(
        f"Total Count           :{duplicated_values.sum()+unique_values.sum()}\n")

    pivot_table_data = pd.DataFrame({"Concat_column_Name": concat_detail_value_count.index,
                                     "counts": concat_detail_value_count.values})

    pivot_table_validation = pd.DataFrame({"Parameters": ['Total Number of Rows', 'Unique rows', 'Duplicated Rows', 'Total'],
                                           "Values": [city_data.shape[0], unique_values.sum(), duplicated_values.sum(), duplicated_values.sum()+unique_values.sum()]})

    list1, list2, list3 = [], [], []
    for index_values in city_data['cluster id'].value_counts().index:
        dp = city_data[city_data['cluster id'] == f'{index_values}']
        for column_name in dp.columns:
            # duplication_count
            duplication_count = dp[f'{column_name}'].duplicated().sum()
            unique_count = dp.shape[0]-1  # unique_count
            if duplication_count != unique_count:
                # print(column_name,unique_count)
                list1.append(column_name)
                list2.append(index_values)
                list3.append(unique_count)

    pivot_table_columns = pd.DataFrame(
        {"Columns_name": list1, 'Concatination': list2, 'counts': list3})

    columns_details = pivot_table_columns['Columns_name'].value_counts()

    pivot_table_summary = pd.DataFrame(
        {"Columns_name": columns_details.index, "Counts": columns_details.values})

    print("*****************************Top 10 Columns Names*****************************")
    print(pivot_table_summary.head(10))
    print(f"\nShape of pivot table summary:{pivot_table_summary.shape}\n")

    if pivot_table_summary.shape[0] < 1:
        _, deduplicate_csv = deduplications_of_city_records(city_data=city_data,
                                                            pivot_table_summary=pivot_table_summary,
                                                            th=1)

    if pivot_table_summary.shape[0] > 1:
        if th == None:
            _, deduplicate_csv = deduplications_of_city_records(city_data=city_data,
                                                                pivot_table_summary=pivot_table_summary,
                                                                th=pivot_table_summary.head(10).Counts.values[-1])
        else:
            _, deduplicate_csv = deduplications_of_city_records(city_data=city_data,
                                                                pivot_table_summary=pivot_table_summary,
                                                                th=th)


    deduplicate_csv['city_index'] = deduplicate_csv.index

    updated_deduplicate_csv = date_formatting(deduplicate_csv)

    # Removing columns having no values
    updated_deduplicate_csv = updated_deduplicate_csv.replace('', np.nan)

    null_columns_list = updated_deduplicate_csv.isnull().sum(
    )[updated_deduplicate_csv.isnull().sum() >= updated_deduplicate_csv.shape[0]].index
    # updated_deduplicate_csv.drop(columns=null_columns_list, inplace=True)

    updated_deduplicate_csv.fillna('', inplace=True)

    # os.path.join(filename,'pivot_table.xlsx')
    with pd.ExcelWriter(os.path.join(filename, 'pivot_table.xlsx')) as writer:
        pivot_table_data.to_excel(
            writer, sheet_name='Pivot_Table', index=False)
        pivot_table_validation.to_excel(
            writer, sheet_name='Pivot_Table_Validation', index=False)
        pivot_table_columns.to_excel(
            writer, sheet_name='Pivot_Table_Columns_Analysis', index=False)
        pivot_table_summary.to_excel(
            writer, sheet_name='Pivot_Table_Summary', index=False)
        updated_deduplicate_csv.to_excel(
            writer, sheet_name='De-Duplication', index=False)

    return updated_deduplicate_csv

# def pivot_table(city_data, dedup_columns=['Business Name','Business Address'], th=None):

#     # Fill NaN values with empty strings in the city_data DataFrame
#     city_data.fillna('', inplace=True)
#     city_data[dedup_columns[1]]=["-" if i=="" else i for i in city_data[dedup_columns[1]]]

#     # Extract Street number from the 'Business Address' column
#     def extract_street_number(address):
#         match = re.match(r'(\d+)', address)
#         return match.group(1) if match else None

#     # Apply the function to extract street number
#     city_data['Street_num'] = city_data[dedup_columns[1]].apply(extract_street_number)
    
#     # Replace empty spaces with None
#     city_data.replace(r'^\s*$', None, regex=True, inplace=True)
    
#     try:
#         indexer = recordlinkage.Index()
#         indexer.block('Street_num')

#         index_pairs = indexer.index(city_data)

#         # Create a Compare object
#         compare = recordlinkage.Compare()
#         compare.string(dedup_columns[0],dedup_columns[0], method='jarowinkler', threshold=0.85, label='Business Name')
#         compare.string(dedup_columns[1],dedup_columns[1], method='jarowinkler', threshold=0.95, label='Business Address')

#         comparison_vectors = compare.compute(index_pairs, city_data)
#         threshold = 2
        
#         matches = comparison_vectors[comparison_vectors.sum(axis=1) >= threshold]

#         # Initialize cluster assignments
#         city_data['cluster id'] = -1

#         # Initialize cluster ID counter
#         cluster_id = 0

#         # Dictionary to map row indices to cluster IDs
#         cluster_map = {}

#         # Process each match to assign cluster IDs
#         for id1, id2 in matches.index:
#             if city_data.loc[id1, 'cluster id'] == -1 and city_data.loc[id2, 'cluster id'] == -1:
#                 # Assign new cluster ID to both records
#                 city_data.loc[id1, 'cluster id'] = cluster_id
#                 city_data.loc[id2, 'cluster id'] = cluster_id
#                 cluster_map[id1] = cluster_id
#                 cluster_map[id2] = cluster_id
#                 cluster_id += 1
#             elif city_data.loc[id1, 'cluster id'] == -1:
#                 # Assign the same cluster ID as the other record
#                 city_data.loc[id1, 'cluster id'] = city_data.loc[id2, 'cluster id']
#                 cluster_map[id1] = city_data.loc[id2, 'cluster id']
#             elif city_data.loc[id2, 'cluster id'] == -1:
#                 # Assign the same cluster ID as the other record
#                 city_data.loc[id2, 'cluster id'] = city_data.loc[id1, 'cluster id']
#                 cluster_map[id2] = city_data.loc[id1, 'cluster id']

#         # Ensure all records are assigned to the correct cluster ID
#         for index in city_data.index:
#             if city_data.loc[index, 'cluster id'] == -1:
#                 # Assign a unique cluster ID if it was not previously assigned
#                 city_data.loc[index, 'cluster id'] = cluster_id
#                 cluster_id += 1

#         city_data = city_data.drop('Street_num', axis=1)
#         city_data.sort_values('cluster id',inplace=True,ascending=True)
#         cluster_dedup = city_data.copy()

#     except ValueError:
#         cluster_dedup=city_data.copy()

#     return cluster_dedup

# def pivot_table(city_data,dedup_columns=['Business Address','Business Name'],th=None):
#     city_data.fillna('',inplace=True)
#     #city_data = city_data.astype(str)
    
#     # remove extra space from strings
#     for cols_names in dedup_columns:
#         bucket_value_store =[]
#         for strings_out in city_data[cols_names].astype(str).values:
#             bucket_value_store.append(strings_out.strip())
#         city_data[cols_names] = bucket_value_store
    
#     #city_data['concat_details'] = city_data['{}'.format(concat_columns[0])]+' '+city_data['{}'.format(concat_columns[1])]
#     city_data['concat_details'] = city_data[dedup_columns].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    
#     abbreviation_data_list = []
#     for abbreviation_filter in  city_data['concat_details']:
#             strings_details = string_filter1(strings=abbreviation_filter)
#             strings_details=strings_details.replace(" ",'')
#             abbreviation_data_list.append(strings_details)
            
#     city_data['cluster id'] = abbreviation_data_list
    
#     return city_data

def pivot_table(city_data, dedup_columns=['Business Name','Business Address'], th=None):
    # Fill NaN values with empty strings in the city_data DataFrame
    city_data.fillna('', inplace=True)
    city_data[dedup_columns[1]]=["-" if i=="" else i for i in city_data[dedup_columns[1]]]
    # Extract Street number from the 'Business Address' column
    address_numeric_parts=[]
    for i in city_data[dedup_columns[1]]:
        numeric_part = next((word for word in i.split() if word.isnumeric()), None)
        address_numeric_parts.append(numeric_part)
    city_data['Street_num']=address_numeric_parts
    # Replace empty spaces with None
    city_data.replace(r'^\s*$', None, regex=True, inplace=True)
    try:
        # Perform deduplication using pandas_dedupe.dedupe_dataframe function
        cluster_records = pandas_dedupe.dedupe_dataframe(city_data,['Street_num',dedup_columns[0],dedup_columns[1]], update_model=False)
        # Drop the 'Street_num' column from the city_data DataFrame
        city_data = city_data.drop('Street_num', axis=1)
        # Concatenate city_data and deduplicated columns ('cluster id' and 'confidence') from cluster_records
        cluster_dedup = pd.concat([city_data, cluster_records[['cluster id','confidence']]], axis=1)
    except ValueError:
        # Drop the 'Street_num' column from the city_data DataFrame
        city_data = city_data.drop('Street_num', axis=1)
        cluster_dedup=city_data
        cluster_dedup['cluster id']=[i for i in range(cluster_dedup.shape[0])]
    cluster_dedup.sort_values('cluster id')
    return cluster_dedup
    
def city_de_duplication(city_path, raw_sheet_name, city_sheet, dedup_columns_list):
    # Set the path to the working file (Raw Sheet)
    working_file = os.path.join(city_path, 'original_record', raw_sheet_name)
    
    # Set the path to the Deduplication Folder
    deduplication_folder_path=os.path.join(city_path, 'results', 'city_data')
    
    # Read the data from the raw data city sheet
    
    city_records = pd.read_excel(working_file, sheet_name=city_sheet)
    
    city_records_update = date_formatting(dataset=city_records)
    
    # Perform deduplication using pivot_table function
    manual_check_dedup= pivot_table(city_data=city_records_update,
                                  dedup_columns=dedup_columns_list,
                                  th=1)
    
    manual_check_dedup.to_excel(os.path.join(deduplication_folder_path,'manual_dedup_records.xlsx'),index=False)


