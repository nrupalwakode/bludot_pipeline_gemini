import pandas as pd
import numpy as np
import pickle
from rapidfuzz import fuzz,process
import os 
import warnings
warnings.filterwarnings("ignore")

global abbreviation_dict
pkl_file_loc = os.path.join(os.getcwd(), 'src', 'updated_pickle.pkl')

abbreviation_dict = pickle.load(open(r'{}'.format(pkl_file_loc), "rb"))
#-------------------------------- Fuzzy Matched --------------------------------------------#
def punctuation_removal(strings):
    strings = strings.lower().replace('{',' ').replace('}',' ').replace('(',' ').replace(')',' ').replace('[',' ').replace(']',' ').replace('.','').replace(',',' ').replace(':','').replace(';','').replace('+',' ').replace('-','').replace('*','').replace('/',' ').replace('&',' ').replace('|',' ').replace('<','').replace('>','').replace('=','').replace('~','').replace('$','').replace('-',' ').replace('_',' ').replace('#','').replace('%','').replace('!','').strip()
    return strings


def string_operation(strings):
    if strings in abbreviation_dict.keys():
       words = strings.replace(strings,abbreviation_dict[strings])
    else:
         words = strings    
    return words
    
def string_filter_for_abbreviation(strings):
    strings  = punctuation_removal(strings=strings)
    strings  = ' '.join([string_operation(strings=word) for word in strings.split()])
    strings  = strings.strip()
    return strings

def string_filter_for_abbreviation1(strings):
    strings  = punctuation_removal(strings=strings)
    strings  = ' '.join([string_operation(strings=word) for word in strings.split()])
    strings  = strings.strip()
    strings1=strings.replace(' ','')
    return strings1

def address_separation(dataset01,dataset02,col1=['Bus address','Address1']):
    dataset1 = dataset01[dataset01['{}'.format(col1[0])].isnull()==False]
    dataset2 = dataset02[dataset02['{}'.format(col1[1])].isnull()==False]
    data1 = dataset1['{}'.format(col1[0])].apply(string_filter_for_abbreviation).str.partition(' ')[0]
    data2 = dataset2['{}'.format(col1[1])].apply(string_filter_for_abbreviation).str.partition(' ')[0]
    data3 = dataset1['{}'.format(col1[0])].apply(string_filter_for_abbreviation).str.partition(' ')[2]
    data4 = dataset2['{}'.format(col1[1])].apply(string_filter_for_abbreviation).str.partition(' ')[2]
    return data1,data2,data3,data4

def selected_rows(dataset1,dataset2,index1,index2):  
    data1 = dataset1[dataset1.index.isin(index1)]
    data2 = dataset2[dataset2.index.isin(index2)]
    return data1,data2

def separate_main_spreadsheet(original_data,city_dataset,bludot_dataset):
    print("city_dataset['city_index'] type:", type(city_dataset['city_index']))
    print("city_dataset['city_index'].shape:", city_dataset['city_index'].shape)
    print("city_dataset['city_index'].values.shape:", city_dataset['city_index'].values.shape)
    print("city_dataset['city_index'].head():\n", city_dataset['city_index'].head())    
    print("original_data['city_index'] type:", type(original_data['city_index']))
    print("original_data['city_index'].shape:", original_data['city_index'].shape)
    print("original_data['city_index'].values.shape:", original_data['city_index'].values.shape)
    print("original_data['city_index'].head():\n", original_data['city_index'].head())
    #print("original_data.columns:", original_data.columns.tolist())   
    additional_city_dataset = city_dataset[~city_dataset.city_index.isin(original_data['city_index'].values)]
    additional_bludot_dataset =  bludot_dataset[~bludot_dataset.bludot_index.isin(original_data['bludot_index'].values)]
    
    return additional_city_dataset,additional_bludot_dataset

def fast_loop_Ratio(value,index,value1,index1,th):
    ratio_matched =[(j,l) for i,j in zip(value,index) for k,l in zip(value1,index1) 
                    if fuzz.ratio(string_filter_for_abbreviation(i),string_filter_for_abbreviation(k))>th]
    
    return ratio_matched

def get_raw_dataframe_format(index1,index2,dataset1,dataset2):
    # Create Dataframe using selected record index mention by table 'dataset1 & dataset2'
    reference_table = pd.DataFrame({"index1":index1,"index2":index2})
    city_table_refer = dataset1.copy()
    city_table_refer['index1']=city_table_refer.index
    bludot_table_refer = dataset2.copy()
    bludot_table_refer['index2']=bludot_table_refer.index
    
    # Used index1 & index2 columns from city & bludot reference table for merging.
    city_table_joint = pd.merge(reference_table,city_table_refer,left_on='index1',right_on='index1')
    bludot_table_joint = pd.merge(city_table_joint,bludot_table_refer,left_on='index2',right_on='index2')
    return bludot_table_joint

def remove_duplicate_UUID(dataset):
    pivot_table_uuid = dataset['UUID'].value_counts()[dataset['UUID'].value_counts()>1]
    
    for uuid_value in pivot_table_uuid.index:
        sample_data = dataset[dataset['UUID']==uuid_value]
        max_score   = sample_data[sample_data['Total_score']==sample_data['Total_score'].max()]
        deleted_index = []
        for uuid_index in list(sample_data.index):
            if uuid_index not in list(max_score.index):
                deleted_index.append(uuid_index)
                
        dataset.drop(index=deleted_index,inplace=True)
       
    dataset = dataset[~dataset['UUID'].duplicated()]
    dataset.index = np.arange(0,dataset.shape[0]) 
    return dataset

def phone_matching(dataset,columns_name,method,types):
    formated_records = dataset.copy()
    updated_records = dataset[columns_name]
    alert_status =[]
    updated_scores = []
    updated_records.fillna("",inplace=True)
    city_name = updated_records[columns_name[2]].apply(string_filter_for_abbreviation)
    bludot_name = updated_records[columns_name[3]].apply(string_filter_for_abbreviation)
    city_phone = updated_records[columns_name[0]].replace(' ','').apply(string_filter_for_abbreviation1)
    bludot_phone = updated_records[columns_name[1]].replace(' ','').apply(string_filter_for_abbreviation1)
    for i,j,n,p,q in zip(city_phone,bludot_phone,updated_records.index,city_name,bludot_name):
        street_number  = fuzz.ratio(i,j)
        street_address = fuzz.ratio(i,j)
        if types == 'Auto':
            
            if method != 'Token_Sorted_Ratio':
                business_name  = fuzz.ratio(p,q)
            #print(street_number,street_address)
            else:
                business_name  = fuzz.token_sort_ratio(p,q)  
            phone_ratio=fuzz.ratio(i,j)
            if i=='' and j=='':
                phone_ratio=0
            if phone_ratio==100 and (business_name>=57):
                alert_status.append('True_Match')
            else:
                alert_status.append('False_Match') 

            updated_scores.append((street_number,street_address,business_name))
        # else:
        #     if method != 'Token_Sorted_Ratio':
        #         business_name  = fuzz.ratio(p,q)
        #     #print(street_number,street_address)
        #     else:
        #         business_name  = fuzz.token_sort_ratio(p,q)  

        #     if (street_number>=85) & (street_address>=40) & (business_name>=40):
        #         alert_status.append('True_Match')
        #     else:
        #         alert_status.append('False_Match') 

        # updated_scores.append((business_name))        
    Street_Number,Remaining_Number,Name_Busin = zip(*updated_scores)    
    formated_records['Review_Status'] = alert_status
    formated_records['Number_From_Address'] = Street_Number
    formated_records['Address_Matchs'] = Remaining_Number
    formated_records['Name_matchs']    = Name_Busin
    formated_records['Total_score']    = (np.array(Street_Number)+np.array(Remaining_Number)+np.array(Name_Busin))/3
    formated_records = formated_records[formated_records['Review_Status']=='True_Match']
    #formated_records = formated_records.sort_values(['Review_Status'],ascending=False)
    
    remove_duplicates = remove_duplicate_UUID(dataset=formated_records)
    #print(formated_records['Review_Status'].value_counts())

    return remove_duplicates

def email_matching(dataset,columns_name,method,types):
    formated_records = dataset.copy()
    updated_records = dataset[columns_name]
    alert_status =[]
    updated_scores = []
    updated_records.fillna("",inplace=True)
    city_name = updated_records[columns_name[2]].apply(string_filter_for_abbreviation)
    bludot_name = updated_records[columns_name[3]].apply(string_filter_for_abbreviation)
    city_email = updated_records[columns_name[0]].apply(string_filter_for_abbreviation)
    bludot_email = updated_records[columns_name[1]].apply(string_filter_for_abbreviation)
    for i,j,n,p,q in zip(city_email,bludot_email,updated_records.index,city_name,bludot_name):
        street_number  = fuzz.ratio(i,j)
        street_address = fuzz.ratio(i,j)
        if types == 'Auto':
            if method != 'Token_Sorted_Ratio':
                business_name  = fuzz.ratio(p,q)
            #print(street_number,street_address)
            else:
                business_name  = fuzz.token_sort_ratio(p,q)  
            email_ratio=fuzz.ratio(i,j)
            if email_ratio==100 and (business_name>=57):
                alert_status.append('True_Match')
            else:
                alert_status.append('False_Match') 

            updated_scores.append((street_number,street_address,business_name))
        # else:
        #     if method != 'Token_Sorted_Ratio':
        #         business_name  = fuzz.ratio(p,q)
        #     #print(street_number,street_address)
        #     else:
        #         business_name  = fuzz.token_sort_ratio(p,q)  

        #     if (street_number>=85) & (street_address>=40) & (business_name>=40):
        #         alert_status.append('True_Match')
        #     else:
        #         alert_status.append('False_Match') 

        # updated_scores.append((business_name))        
    Street_Number,Remaining_Number,Name_Busin = zip(*updated_scores)    
    formated_records['Review_Status'] = alert_status
    formated_records['Number_From_Address'] = Street_Number
    formated_records['Address_Matchs'] = Remaining_Number
    formated_records['Name_matchs']    = Name_Busin
    formated_records['Total_score']    = (np.array(Street_Number)+np.array(Remaining_Number)+np.array(Name_Busin))/3
    formated_records = formated_records[formated_records['Review_Status']=='True_Match']
    #formated_records = formated_records.sort_values(['Review_Status'],ascending=False)
    
    remove_duplicates = remove_duplicate_UUID(dataset=formated_records)
    #print(formated_records['Review_Status'].value_counts())

    return remove_duplicates

def address_matching(dataset,columns_name,method,types):
    formated_records = dataset.copy()
    # print(dataset.columns)
    updated_records = dataset[columns_name]
    alert_status =[]
    updated_scores = []
    updated_records.fillna("",inplace=True)
    
    add_city_record        = updated_records[columns_name[0]].str.partition(' ')[0]
    add_bludot_record      = updated_records[columns_name[1]].str.partition(' ')[0]
    remaing_city_record    = [string_filter_for_abbreviation(strings) for strings in updated_records[columns_name[0]].str.partition(' ')[2].values]
    remaing_bludot_record  = [string_filter_for_abbreviation(strings) for strings in updated_records[columns_name[1]].str.partition(' ')[2].values]
    
    city_name = updated_records[columns_name[2]].apply(string_filter_for_abbreviation)
    bludot_name = updated_records[columns_name[3]].apply(string_filter_for_abbreviation)
    
    for i,j,k,l,n,p,q in zip(add_city_record,add_bludot_record,remaing_city_record,remaing_bludot_record,updated_records.index,city_name,bludot_name):
        original_address_parts = i.split(" ")
        new_address_parts = j.split(" ")
        original_address_numeric_part, new_address_numeric_part = '', ''
        for part in original_address_parts:
            if part.isnumeric():
                original_address_numeric_part = part
                break
        for part in new_address_parts:
            if part.isnumeric():
                new_address_numeric_part = part
                break
        
        street_number  = fuzz.ratio(original_address_numeric_part,new_address_numeric_part)
        street_address = fuzz.ratio(k,l)
        if types == 'Auto':
            if method != 'Token_Sorted_Ratio':
                business_name  = fuzz.ratio(p,q)
            #print(street_number,street_address)
            else:
                business_name  = fuzz.token_sort_ratio(p,q)  

            if ((street_number==100) or original_address_numeric_part=='' or new_address_numeric_part=='') and (street_address>=57) and (business_name>=57):
                alert_status.append('True_Match')
            elif (original_address_numeric_part=='' or new_address_numeric_part=='') and (business_name>=57):
                alert_status.append('True_Match')
            else:
                alert_status.append('False_Match') 

            

            updated_scores.append((street_number,street_address,business_name))
        else:
            if method != 'Token_Sorted_Ratio':
                business_name  = fuzz.ratio(p,q)
            #print(street_number,street_address)
            else:
                business_name  = fuzz.token_sort_ratio(p,q)  

            if (street_number==100) & (street_address>=40) & (business_name>=40):
                alert_status.append('True_Match')
            else:
                alert_status.append('False_Match') 

            updated_scores.append((street_number,street_address,business_name))        
    Street_Number,Remaining_Number,Name_Busin = zip(*updated_scores)    
    formated_records['Review_Status'] = alert_status
    formated_records['Number_From_Address'] = Street_Number
    formated_records['Address_Matchs'] = Remaining_Number
    formated_records['Name_matchs']    = Name_Busin
    formated_records['Total_score']    = (np.array(Street_Number)+np.array(Remaining_Number)+np.array(Name_Busin))/3
    formated_records = formated_records[formated_records['Review_Status']=='True_Match']
    #formated_records = formated_records.sort_values(['Review_Status'],ascending=False)
    
    remove_duplicates = remove_duplicate_UUID(dataset=formated_records)
    #print(formated_records['Review_Status'].value_counts())

    return remove_duplicates

def name_matching(dataset,columns_name,method,types):
        formated_records = dataset.copy()
        updated_records = dataset[columns_name]
        alert_status =[]
        updated_scores = []
        updated_records.fillna("",inplace=True)
         
        city_address = updated_records[columns_name[0]].apply(string_filter_for_abbreviation)
        bludot_address = updated_records[columns_name[1]].apply(string_filter_for_abbreviation) 

        city_name = updated_records[columns_name[2]].apply(string_filter_for_abbreviation)
        bludot_name = updated_records[columns_name[3]].apply(string_filter_for_abbreviation)
        
        for i,j,n,p,q in zip(city_address,bludot_address,updated_records.index,city_name,bludot_name):
    
            if types == 'auto':
                if method != 'Token_Sorted_Ratio':
                    business_name  = fuzz.ratio(p,q)
                #print(street_number,street_address)
                else:
                    business_name  = fuzz.token_sort_ratio(p,q)  

                if (i=='' or j=='') and (business_name>=70):
                    alert_status.append('True_Match')
                else:
                    alert_status.append('False_Match') 
                # print(i,'..................',j)

                updated_scores.append((100,100,business_name))
            
            elif types == 'manual':
                if method != 'Token_Sorted_Ratio':
                    business_name  = fuzz.ratio(p,q)
                #print(street_number,street_address)
                else:
                    business_name  = fuzz.token_sort_ratio(p,q)  

                if (i=='' or j=='' or i=='-' or j=='-' ) and (business_name>=50):
                    alert_status.append('True_Match')
                else:
                    alert_status.append('False_Match') 
                

                updated_scores.append((100,100,business_name))
        Street_Number,Remaining_Number,Name_Busin = zip(*updated_scores)    
        formated_records['Review_Status'] = alert_status
        formated_records['Number_From_Address'] = Street_Number
        formated_records['Address_Matchs'] = Remaining_Number
        formated_records['Name_matchs']    = Name_Busin
        formated_records['Total_score']    = (np.array(Street_Number)+np.array(Remaining_Number)+np.array(Name_Busin))/3
        formated_records = formated_records[formated_records['Review_Status']=='True_Match']
        #formated_records = formated_records.sort_values(['Review_Status'],ascending=False)
        
        remove_duplicates = remove_duplicate_UUID(dataset=formated_records)
        #print(formated_records['Review_Status'].value_counts())

        return remove_duplicates

def filter_data_based_on_length(dataset,col1=['Business Name','Name']):
       
    dataset.index=np.arange(0,dataset.shape[0])
    index_value =[]
    for selected_index in range(0,dataset.shape[0]):
       
        status,status1 = [],[]
        table1_selected_value = punctuation_removal(string_filter_for_abbreviation(dataset[col1[0]].values[selected_index].lower()))        
        table2_selected_value = punctuation_removal(string_filter_for_abbreviation(dataset[col1[1]].values[selected_index].lower()))

        if len(table1_selected_value.split())>=len(table2_selected_value.split()):
            for i in table2_selected_value.split():
                if i in table1_selected_value.split():
                    status.append(1)
                else:
                     if len(i)>1:
                         query=i
                         choices = table1_selected_value.split()
                         th = process.extractOne(query, choices)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th>90:
                            status.append(1)  
                         else:
                             status.append(0)
                     else:
                             status.append(0)                
        else:
             for j in table1_selected_value.split():
                if j in table2_selected_value.split():
                    status1.append(1)
                else:
                     if len(j)>1:
                         query=j
                         choices = table2_selected_value.split()
                         th1 = process.extractOne(query, choices)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th1>90:
                            status1.append(1)  
                         else:
                             status1.append(0)
                     else:
                          status1.append(0)   
        
        th_a       = len(status)
        variable_a = abs(int(np.array(status).sum())-len(status))
        th_b       = len(status1)
        variable_b = abs(int(np.array(status1).sum())-len(status1))
        
        if ((th_a==0) & (variable_b<1)) | ((th_b==0) & (variable_a<1)):

            #print("Selected records by condition1.......................")
            #print(status,status1,selected_index,'<--->',table1_selected_value,'<---->',table2_selected_value,'--',len(table1_selected_value),len(table2_selected_value))                  
            index_value.append(selected_index)
            
    updated_records = dataset[dataset.index.isin(index_value)]
    updated_records['Review_Status']='True_Match'
    
    unselected_records = dataset[~dataset.index.isin(index_value)]
    unselected_records['Review_Status']='Manual_Match'

    return updated_records,unselected_records

def index_based_deduplications(dataset,default_cols='index1'):
    pivot_table = dataset[default_cols].value_counts()
    duplicated_records = [f for f in pivot_table[pivot_table>1].index]
    #print(duplicated_records)
    
    remove_duplicated_records =[]
    for dup_record_inter in duplicated_records:
        #print(dup_record_inter)
        sample_dataset       = dataset[dataset[default_cols].isin([dup_record_inter])]
        threshold_base_index = sample_dataset[sample_dataset['Total_score']==sample_dataset['Total_score'].max()].index
        
        if len(threshold_base_index)>1:
            selected_records = threshold_base_index[0]
        else:
            selected_records = threshold_base_index
        
        
        for records in sample_dataset.index: 
            if records!=selected_records:
               remove_duplicated_records.append(records)

    #print(remove_duplicated_records)    
    dataset.drop(index=remove_duplicated_records,inplace=True)
    return dataset
def fast_loop_Ratio(value,index,value1,index1,th):
    ratio_matched =[(j,l) for i,j in zip(value,index) for k,l in zip(value1,index1) 
                    if fuzz.ratio(i,k)>th]
    return ratio_matched

def fast_loop_token_sort_ratio(value,index,value1,index1,th):
    token_sort_ratio =[(j,l) for i,j in zip(value,index) for k,l in zip(value1,index1) 
                       if fuzz.token_sort_ratio(i, k)>th]
    
    return token_sort_ratio

def strings_matching(city_records,bludot_records,city_records_cols,bludot_records_cols,fuzzy_th,method,mode=None,rule='rule1'):
    print(mode)
    for city_cols in city_records_cols:
        city_records[city_cols].fillna('-',inplace=True)
        
    for bludot_cols in bludot_records_cols:
        bludot_records[bludot_cols].fillna('-',inplace=True)
        
    city_records.index=np.arange(0,city_records.shape[0])
    bludot_records.index = np.arange(0,bludot_records.shape[0])
    
    updated_city_name_cols = city_records[city_records_cols[0]].apply(string_filter_for_abbreviation)
    updated_bludot_name_cols = bludot_records[bludot_records_cols[0]].apply(string_filter_for_abbreviation)
    business_name_summary =[]
    if method=='Ratio':
        scorer = fuzz.ratio
        print('ratio')
        business_name_summary = fast_loop_Ratio(value=updated_city_name_cols.values,
                                                index=updated_city_name_cols.index,
                                                value1=updated_bludot_name_cols.values,
                                                index1=updated_bludot_name_cols.index,
                                                th=fuzzy_th)
    if method=='WRatio':
        for city_index in city_records.index:
            fuzzy_matching_name = process.extractOne(updated_city_name_cols.loc[city_index,],
                                                     updated_bludot_name_cols,
                                                     )

            if fuzzy_matching_name[1]>fuzzy_th:
                business_name_summary.append((city_index,fuzzy_matching_name[2]))  
            
    if method=='Token_Sorted_Ratio':
        scorer = fuzz.token_sort_ratio  
        print('Token_Sorted_Ratio')   
        business_name_summary = fast_loop_token_sort_ratio(value=updated_city_name_cols.values,
                                                index=updated_city_name_cols.index,
                                                value1=updated_bludot_name_cols.values,
                                                index1=updated_bludot_name_cols.index,
                                                th=fuzzy_th)
    print(len(business_name_summary))
    if len(business_name_summary)>=1:        
        selected_city_index,selected_bludot_index = zip(*business_name_summary)
        print(selected_city_index,selected_bludot_index)
        city_record_business_add, bludot_record_business_add = selected_rows(dataset1 = city_records,
                                                                             dataset2 = bludot_records,
                                                                             index1   = selected_city_index,
                                                                             index2   = selected_bludot_index)

        fuzzy_matched_report = get_raw_dataframe_format(index1=selected_city_index,
                                                        index2=selected_bludot_index,
                                                        dataset1=city_records,
                                                        dataset2=bludot_records)
        if rule=='rule1':                                        
            if mode=='auto':
                updated_Records_with_fuzzy_scores = address_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types='Auto')

            elif mode=='manual':
                updated_Records_with_fuzzy_scores = address_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types=None)
        if rule=='rule3':
            if mode=='auto':
                updated_Records_with_fuzzy_scores = phone_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types='Auto')

            elif mode=='manual':
                updated_Records_with_fuzzy_scores = phone_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types=None)

        if rule=='rule5':      
            # print(mode)                                         
            if mode=='auto':
                updated_Records_with_fuzzy_scores = name_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types='auto')

            elif mode=='manual':
                updated_Records_with_fuzzy_scores = name_matching(dataset=fuzzy_matched_report,
                                                                    columns_name=[city_records_cols[1],bludot_records_cols[1],city_records_cols[0],bludot_records_cols[0]],
                                                                    method=method,
                                                                    types='manual')
            
        unselected_city_records,unselected_bludot_records = separate_main_spreadsheet(original_data=updated_Records_with_fuzzy_scores,
                                                                                      city_dataset=city_records,
                                                                                      bludot_dataset=bludot_records)
        
        updated_Records_with_fuzzy_scores = updated_Records_with_fuzzy_scores.sort_values(['Address_Matchs','Name_matchs'],ascending=[False,False])
        
        if mode==None:
            selected_records,unselected_records = filter_data_based_on_length(dataset=updated_Records_with_fuzzy_scores,
                                                  col1=[city_records_cols[0],bludot_records_cols[0]])

            manual_records_with_fuzzy_matched = pd.concat([selected_records,unselected_records])
                  
            return manual_records_with_fuzzy_matched,unselected_city_records,unselected_bludot_records
        
        return updated_Records_with_fuzzy_scores,unselected_city_records,unselected_bludot_records
    
    else:
        updated_Records_with_fuzzy_scores = pd.DataFrame()
        
        return updated_Records_with_fuzzy_scores,city_records,bludot_records

def fuzzy_based_string_matching1(city_records,bludot_records,city_records_cols,bludot_records_cols,file_path,mode,rule,runs='single'):
    if mode=='auto':
        fuzzy_th =50
    elif mode=='manual':
        fuzzy_th =40
    additional_columns = ['index1','index2','Review_Status',
                         'Number_From_Address','Address_Matchs','Name_matchs','Total_score']

    updated_cols_details = ['UUID']
    for remaining_cols in np.concatenate([city_records.columns,bludot_records.columns]):
        if remaining_cols not in additional_columns:
            updated_cols_details.append(remaining_cols)
            
    for add_cols in additional_columns:
        updated_cols_details.append(add_cols)
        
    if ((city_records.shape[0]!=0) & (bludot_records.shape[0]!=0)):
        # city_records_cols=['Company Name_1','Address1_1'] 
        # bludot_records_cols=['Name', 'Address1']
        
        fuzzy_matched_report,unselected_city_records,unselected_bludot_records = strings_matching(city_records        = city_records,
                                                                                                  bludot_records      = bludot_records,
                                                                                                  city_records_cols   = city_records_cols,
                                                                                                  bludot_records_cols = bludot_records_cols,
                                                                                                  method              = 'Ratio',
                                                                                                  fuzzy_th            = fuzzy_th,
                                                                                                  mode               ='Auto',
                                                                                                  rule=rule) 
        print("********* Fuzzy Matching Using Ratio Method *********")
        print("-----------------------------------------------------")
        print('True_Matches  :',fuzzy_matched_report['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records.shape[0])
        print("-----------------------------------------------------")
    else:  
         additional_columns        = ['index1','index2','Review_Status',
                                      'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
         updated_columns_names     = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
         fuzzy_matched_report      = pd.DataFrame(columns=updated_columns_names)
         unselected_city_records   = city_records.copy()
         unselected_bludot_records = bludot_records.copy()
            
    # if  (unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0) & (types=='Auto'):     
    #     fuzzy_matched_report1,unselected_city_records1,unselected_bludot_records1 = strings_matching(city_records        = unselected_city_records,
    #                                                                                                  bludot_records      = unselected_bludot_records,
    #                                                                                                  city_records_cols   = city_records_cols,
    #                                                                                                  bludot_records_cols = bludot_records_cols,
    #                                                                                                  method    = 'WRatio',
    #                                                                                                  fuzzy_th  = fuzzy_th,
    #                                                                                                  types     = 'Auto')
    #     print("********* Fuzzy Matching Using WRatio Method ********")
    #     print("-----------------------------------------------------")
    #     print('True_Matches  :',fuzzy_matched_report1['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records1.shape[0])
    #     print("-----------------------------------------------------") 
   
    # else:
    #      additional_columns        = ['city_index','bludot_index','index1','index2','Review_Status',
    #                                   'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
    #      updated_columns_names      = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
    #      fuzzy_matched_report1      = pd.DataFrame(columns=updated_columns_names)
    #      unselected_city_records1   = unselected_city_records.copy()
    #      unselected_bludot_records1 = unselected_bludot_records.copy()  
   
    
    if  (unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0): 
        if mode=='auto':
            fuzzy_th =50
        elif mode=='manual':
            fuzzy_th =40
        additional_columns = ['index1','index2','Review_Status',
                            'Number_From_Address','Address_Matchs','Name_matchs','Total_score']

        updated_cols_details = ['UUID']
        for city_cols in [city_records_cols[0],bludot_records_cols[0],city_records_cols[1],bludot_records_cols[1]]:
            updated_cols_details.append(city_cols)    
        
        fuzzy_matched_report2,unselected_city_records2,unselected_bludot_records2 = strings_matching(city_records       = unselected_city_records,
                                                                                                     bludot_records     = unselected_bludot_records,
                                                                                                     city_records_cols  = city_records_cols,
                                                                                                     bludot_records_cols= bludot_records_cols,
                                                                                                     method             = 'Token_Sorted_Ratio',
                                                                                                     fuzzy_th           = fuzzy_th,
                                                                                                     mode              = 'auto',
                                                                                                     rule=rule)  
        
        print("********* Fuzzy Matching Using Token Sorted Method *********")
        print("-----------------------------------------------------")
        print('True_Matches  :',fuzzy_matched_report2['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records.shape[0])
        print("-----------------------------------------------------") 
        
    else:
         additional_columns        = ['index1','index2','Review_Status',
                                      'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
         updated_columns_names      = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
         fuzzy_matched_report2      = pd.DataFrame(columns=updated_columns_names)
         unselected_city_records1   = unselected_city_records.copy()
         unselected_bludot_records1 = unselected_bludot_records.copy() 
   
    if mode == 'manual':
        # print('Entered manual')
        if ((unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0) & (mode == 'Manual')):
            fuzzy_matched_report3,unselected_city_records3,unselected_bludot_records3 = strings_matching(city_records        = unselected_city_records,
                                                                                                         bludot_records      = unselected_bludot_records,
                                                                                                         city_records_cols   = city_records_cols,
                                                                                                         bludot_records_cols = bludot_records_cols,
                                                                                                         method   = 'Ratio',
                                                                                                         fuzzy_th = 40,
                                                                                                         mode='manual',
                                                                                                         rule=rule)
            
            print("********* Fuzzy Matching Using Ratio Method *********")
            print("-----------------------------------------------------")
            print('True_Matches  :',fuzzy_matched_report3['Review_Status'].value_counts().values,'\nManual_Matches:',unselected_city_records3.shape[0])
            print("-----------------------------------------------------") 
            
            manual_fuzzy_matched_records  = fuzzy_matched_report3[fuzzy_matched_report3['Review_Status']=='True_Match']
            manual_fuzzy_unmatched_records= fuzzy_matched_report3[fuzzy_matched_report3['Review_Status']=='Manual_Match']
    

def fuzzy_based_string_matching(city_records,bludot_records,city_records_cols,bludot_records_cols,file_path,mode,rule,runs='single'):
    print(mode)
    #types ='Auto'
    if mode=='auto':
        fuzzy_th =50
    elif mode=='manual':
        fuzzy_th =40
    # print(fuzzy_th)
    additional_columns = ['index1','index2','Review_Status',
                         'Number_From_Address','Address_Matchs','Name_matchs','Total_score']

    updated_cols_details = ['UUID']
    for city_cols in [city_records_cols[0],bludot_records_cols[0],city_records_cols[1],bludot_records_cols[1]]:
        updated_cols_details.append(city_cols)
        
#     for bludot_cols in bludot_records_cols:
#         updated_cols_details.append(bludot_cols)   
        
    for remaining_cols in np.concatenate([city_records.columns,bludot_records.columns]):
        if remaining_cols not in additional_columns:
            updated_cols_details.append(remaining_cols)
    #print('updated_columns1:',updated_cols_details)        
    for add_cols in additional_columns:
        updated_cols_details.append(add_cols)
    #print('updated_columns2:',updated_cols_details)  

        
    if ((city_records.shape[0]!=0) & (bludot_records.shape[0]!=0)):
        # city_records_cols=['Company Name_1','Address1_1'] 
        # bludot_records_cols=['Name', 'Address1']
        
        fuzzy_matched_report,unselected_city_records,unselected_bludot_records = strings_matching(city_records        = city_records,
                                                                                                  bludot_records      = bludot_records,
                                                                                                  city_records_cols   = city_records_cols,
                                                                                                  bludot_records_cols = bludot_records_cols,
                                                                                                  method              = 'Ratio',
                                                                                                  fuzzy_th            = fuzzy_th,
                                                                                                  mode               =mode,
                                                                                                  rule=rule) 
        print("********* Fuzzy Matching Using Ratio Method *********")
        print("-----------------------------------------------------")
        print('True_Matches  :',fuzzy_matched_report['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records.shape[0])
        print("-----------------------------------------------------")
    else:  
         additional_columns        = ['index1','index2','Review_Status',
                                      'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
         updated_columns_names     = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
         fuzzy_matched_report      = pd.DataFrame(columns=updated_columns_names)
         unselected_city_records   = city_records.copy()
         unselected_bludot_records = bludot_records.copy()
    print('fuzzy_matched_report:',fuzzy_matched_report.columns) 
    # if  (unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0) & (types=='Auto'):     
    #     fuzzy_matched_report1,unselected_city_records1,unselected_bludot_records1 = strings_matching(city_records        = unselected_city_records,
    #                                                                                                  bludot_records      = unselected_bludot_records,
    #                                                                                                  city_records_cols   = city_records_cols,
    #                                                                                                  bludot_records_cols = bludot_records_cols,
    #                                                                                                  method    = 'WRatio',
    #                                                                                                  fuzzy_th  = fuzzy_th,
    #                                                                                                  types     = 'Auto')
    #     print("********* Fuzzy Matching Using WRatio Method ********")
    #     print("-----------------------------------------------------")
    #     print('True_Matches  :',fuzzy_matched_report1['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records1.shape[0])
    #     print("-----------------------------------------------------") 
   
    # else:
    #      additional_columns        = ['city_index','bludot_index','index1','index2','Review_Status',
    #                                   'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
    #      updated_columns_names      = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
    #      fuzzy_matched_report1      = pd.DataFrame(columns=updated_columns_names)
    #      unselected_city_records1   = unselected_city_records.copy()
    #      unselected_bludot_records1 = unselected_bludot_records.copy()  
   
    
    if  (unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0):     
        
        fuzzy_matched_report2,unselected_city_records2,unselected_bludot_records2 = strings_matching(city_records       = unselected_city_records,
                                                                                                     bludot_records     = unselected_bludot_records,
                                                                                                     city_records_cols  = city_records_cols,
                                                                                                     bludot_records_cols= bludot_records_cols,
                                                                                                     method             = 'Token_Sorted_Ratio',
                                                                                                     fuzzy_th           = fuzzy_th,
                                                                                                     mode              = mode,
                                                                                                     rule=rule)  
        
        print("********* Fuzzy Matching Using Token Sorted Method *********")
        print("-----------------------------------------------------")
        print('True_Matches  :',fuzzy_matched_report2['Review_Status'].value_counts().values,'\nFalse_Matches :',unselected_city_records.shape[0])
        print("-----------------------------------------------------") 
        
    else:
         additional_columns        = ['index1','index2','Review_Status',
                                      'Number_From_Address','Address_Matchs','Name_matchs','Total_score']
            
         updated_columns_names      = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
         fuzzy_matched_report2      = pd.DataFrame(columns=updated_columns_names)
         unselected_city_records1   = unselected_city_records.copy()
         unselected_bludot_records1 = unselected_bludot_records.copy() 
    print('fuzzy_matched_report2:',fuzzy_matched_report2.columns) 
    if mode == None:
        # print('Entered manual')
        if ((unselected_city_records.shape[0]!=0) & (unselected_bludot_records.shape[0]!=0) & (mode == 'Manual')):
            fuzzy_matched_report3,unselected_city_records3,unselected_bludot_records3 = strings_matching(city_records        = unselected_city_records,
                                                                                                         bludot_records      = unselected_bludot_records,
                                                                                                         city_records_cols   = city_records_cols,
                                                                                                         bludot_records_cols = bludot_records_cols,
                                                                                                         method   = 'Ratio',
                                                                                                         fuzzy_th = 40,
                                                                                                         mode='manual',
                                                                                                         rule=rule)
            
            print("********* Fuzzy Matching Using Ratio Method *********")
            print("-----------------------------------------------------")
            print('True_Matches  :',fuzzy_matched_report3['Review_Status'].value_counts().values,'\nManual_Matches:',unselected_city_records3.shape[0])
            print("-----------------------------------------------------") 
            
            manual_fuzzy_matched_records  = fuzzy_matched_report3[fuzzy_matched_report3['Review_Status']=='True_Match']
            manual_fuzzy_unmatched_records= fuzzy_matched_report3[fuzzy_matched_report3['Review_Status']=='Manual_Match']
            
        # else:
        #      additional_columns        = ['city_index','bludot_index','index1','index2','Review_Status',
        #                                   'Number_From_Address','Address_Matchs','Name_matchs','Total_score']

        #      updated_columns_names      = np.concatenate([city_records.columns,bludot_records.columns,additional_columns]) 
        #      fuzzy_matched_report3      = pd.DataFrame(columns=updated_columns_names)
        #      unselected_city_records3   = unselected_city_records1.copy()
        #      unselected_bludot_records3 = unselected_bludot_records1.copy()
        #      manual_fuzzy_matched_records = pd.DataFrame(columns=updated_columns_names)
        #      manual_fuzzy_unmatched_records = pd.DataFrame(columns=updated_columns_names)
            print('fuzzy_matched_report3:',fuzzy_matched_report3.columns)        
       
            final_fuzzy_matched_records = manual_fuzzy_matched_records
            
            final_fuzzy_matched_records = final_fuzzy_matched_records[updated_cols_details]
            
            final_fuzzy_matched_records.index = np.arange(0,final_fuzzy_matched_records.shape[0])
            if runs !='single':
                manual_fuzzy_unmatched_records = manual_fuzzy_unmatched_records[updated_cols_details]
                
                
                manual_fuzzy_unmatched_records.to_excel(os.path.join(file_path,'Manual_Matched_Records.xlsx'),                             
                                            sheet_name='Manual_Matched_Records',
                                            index=False)
        
        
        
        #print(updated_final_fuzzy_matched_records.shape)
        
            final_fuzzy_matched_records.to_excel(os.path.join(file_path,'Total_Matched_Records.xlsx'),                             
                                        sheet_name='Total_Matched_Records',
                                        index=False) 

            additional_city_record,additional_bludot_records =  separate_main_spreadsheet(original_data = final_fuzzy_matched_records,
                                                                                        city_dataset  = city_records,
                                                                                        bludot_dataset= bludot_records)

        
            additional_city_record.to_excel(os.path.join(file_path,'Additional_City_Records.xlsx'),
                                        sheet_name='Additional_City_Records',
                                        index=False)
            
            additional_bludot_records.to_excel(os.path.join(file_path,'Additional_Bludot_Records.xlsx'),
                                        sheet_name='Additional_Bludot_Records',
                                        index=False)
            return final_fuzzy_matched_records
    else:
    
        # final_fuzzy_matched_records = pd.concat([fuzzy_matched_report,
        #                                         fuzzy_matched_report2],
        #                                         axis=0)
        final_fuzzy_matched_records = fuzzy_matched_report
        
        final_fuzzy_matched_records = final_fuzzy_matched_records[updated_cols_details]
        
        final_fuzzy_matched_records.index = np.arange(0,final_fuzzy_matched_records.shape[0])
        updated_final_fuzzy_matched_records = index_based_deduplications(dataset=final_fuzzy_matched_records)
        updated_final_fuzzy_matched_records.to_excel(os.path.join(file_path,'Total_Matched_Records.xlsx'),                             
                                   sheet_name='Total_Matched_Records',
                                   index=False) 

        additional_city_record,additional_bludot_records =  separate_main_spreadsheet(original_data = updated_final_fuzzy_matched_records,
                                                                                    city_dataset  = city_records,
                                                                                    bludot_dataset= bludot_records)

    
        additional_city_record.to_excel(os.path.join(file_path,'Additional_City_Records.xlsx'),
                                    sheet_name='Additional_City_Records',
                                    index=False)
        
        additional_bludot_records.to_excel(os.path.join(file_path,'Additional_Bludot_Records.xlsx'),
                                    sheet_name='Additional_Bludot_Records',
                                    index=False)

    
    
    # duplicated_records.to_excel(r"{}\Deduplicated_Records.xlsx".format(file_path),
    #                                sheet_name='Deduplicated_Records',
    #                                index=False)

    #print("Deduplicated Records Founds:{}".format(duplicated_records.shape))
        print("Selected Records:{}".format(updated_final_fuzzy_matched_records.shape))
        print("Additional City Records:{}".format(additional_city_record.shape))
        print("Additional Bludot Records:{}".format(additional_bludot_records.shape))

        return updated_final_fuzzy_matched_records   