import pandas as pd
import numpy as np
from rapidfuzz import fuzz,process 
import warnings
import pickle
import os

warnings.filterwarnings("ignore")

pkl_file_loc = os.path.join(os.getcwd(),'src','updated_pickle.pkl')

global abbreviation_dict
abbreviation_dict = pickle.load(open(r'{}'.format(pkl_file_loc), "rb"))

def string_filter(strings):
    strings = strings.lower().replace('{',' ').replace('}',' ').replace('(',' ').replace(')',' ').replace('[',' ').replace(']',' ').replace('.','').replace(',',' ').replace(':','').replace(';','').replace('+',' ').replace('-','').replace('*','').replace('/',' ').replace('&',' ').replace('|',' ').replace('<','').replace('>','').replace('=','').replace('~','').replace('$','').replace('-',' ').replace('_',' ').replace('#','').replace('%','').replace('!','').strip()
    return strings

def string_operation(strings):
    if strings in abbreviation_dict.keys():
       words = strings.replace(strings,abbreviation_dict[strings])
    else:
         words = strings    
    return words
    
def string_filter1(strings):
    strings  = string_filter(strings=strings)
    strings =' '.join([string_operation(strings=word) for word in strings.split()])
    return strings

def filter_partial_ratio_methods(dataset,col1=['Business Name','Name']):
       
    dataset.index=np.arange(0,dataset.shape[0])
    index_value =[]
    for selected_index in range(0,dataset.shape[0]):
       
        status,status1 = [],[]
        table1_selected_value = string_filter1(dataset[col1[0]].values[selected_index].lower())       
        table2_selected_value = string_filter1(dataset[col1[1]].values[selected_index].lower())

        if len(table1_selected_value.split())>=len(table2_selected_value.split()):
            for i in table2_selected_value.split():
                if i in table1_selected_value.split():
                    status.append(1)
                else:
                     if len(i)>=1:
                         query=i
                         choices = table1_selected_value.split()
                         th = process.extractOne(query, choices,scorer=fuzz.partial_ratio)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th>=95:
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
                     if len(j)>=1:
                         query=j
                         choices = table2_selected_value.split()
                         th1 = process.extractOne(query, choices,scorer=fuzz.partial_ratio)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th1>=95:
                            status1.append(1)  
                         else:
                             status1.append(0)
                     else:
                          status1.append(0)  
                            
        variable_A = int(np.array(status).sum())
        variable_B = int(np.array(status1).sum())
        th_A       = len(status)
        th_B       = len(status1)
                         
        if (variable_A>=th_A) & (variable_B>=th_B):
            #print("Selected records by condition1")
             #print(status,status1,selected_index,'<------------------->',table1_selected_value,'<---->',table2_selected_value,'<------------>',len(table1_selected_value),len(table2_selected_value))
             index_value.append(selected_index)    
        
        if (th_A>=4) | (th_B>=4):
            if (variable_A>=(th_A-1)) & (variable_B>=(th_B-1)):
                #print("Selected records by condition1")
                #print(status,status1,selected_index,'<------------------->',table1_selected_value,'<---->',table2_selected_value,'<------------>',len(table1_selected_value),len(table2_selected_value))
                 index_value.append(selected_index)     
            
    updated_records = dataset[dataset.index.isin(index_value)]
    updated_records['Review_Status']='True_Match'
    
    unselected_records = dataset[~dataset.index.isin(index_value)]
    unselected_records['Review_Status']='Manual_Match'
    
    #print(len(index_value),updated_records.shape) 
    return updated_records,unselected_records

def filter_ratio_method(dataset,col1=['Business Name','Name']):
       
    dataset.index=np.arange(0,dataset.shape[0])
    index_value =[]
    for selected_index in range(0,dataset.shape[0]):
       
        status,status1 = [],[]
        table1_selected_value = string_filter1(dataset[col1[0]].values[selected_index].lower())       
        table2_selected_value = string_filter1(dataset[col1[1]].values[selected_index].lower())

        if len(table1_selected_value.split())>=len(table2_selected_value.split()):
            for i in table2_selected_value.split():
                if i in table1_selected_value.split():
                    status.append(1)
                else:
                     if len(i)>1:
                         query=i
                         choices = table1_selected_value.split()
                         th = process.extractOne(query, choices,scorer=fuzz.ratio)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th>=88:
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
                         th1 = process.extractOne(query, choices,scorer=fuzz.ratio)[1] 
                         #print(process.extractOne(query, choices)[1])
                         if th1>=88:
                            status1.append(1)  
                         else:
                             status1.append(0)
                     else:
                          status1.append(0)  
                            
        #print(status,status1,selected_index,'<------------------->',table1_selected_value,'<---->',table2_selected_value,'<------------>',len(table1_selected_value),len(table2_selected_value))                  

        if (int(np.array(status).sum())==len(status)) & (int(np.array(status1).sum())==len(status1)):
            #print("Selected records by condition1")
            index_value.append(selected_index)            
    
    updated_records = dataset[dataset.index.isin(index_value)]
    updated_records['Review_Status']='True_Match'
    
    unselected_records = dataset[~dataset.index.isin(index_value)]
    unselected_records['Review_Status']='Manual_Match'
    
    #print(len(index_value),updated_records.shape) 
    return updated_records,unselected_records

def filter_partial_ratio(dataset,col1=['Business Name','Name']):
    partial_ratio_list =[]
    
    for j,k,l in zip(dataset[col1[0]].values,dataset[col1[1]].values,dataset[col1[0]].index):
        string1 = string_filter1(j.lower())       
        string2 = string_filter1(k.lower())
        
        scores = fuzz.partial_ratio(string1,string2)
        if scores>=92:
            #print(scores,l)
            partial_ratio_list.append(l)
            
    partial_ratio_selected_table = dataset[dataset.index.isin(partial_ratio_list)]
    partial_ratio_unmatch_table  = dataset[~dataset.index.isin(partial_ratio_list)] 

    partial_ratio_selected_table['Review_Status'] = 'True_Match'
    partial_ratio_unmatch_table['Review_Status']  = 'Manual_Match'

    return partial_ratio_selected_table,partial_ratio_unmatch_table


def filter_ratio(dataset,col1=['Business Name','Name']):
    ratio_list=[]
    for j,k,l in zip(dataset[col1[0]].values, dataset[col1[1]].values, dataset[col1[0]].index):

        string1 = string_filter1(j.lower())       
        string2 = string_filter1(k.lower())
        
        scores = fuzz.ratio(string1,string2)
        
        if scores>=84:
            #print(scores,l)
            ratio_list.append(l) 
            
    ratio_selected_table = dataset[dataset.index.isin(ratio_list)]
    ratio_unmatch_table  = dataset[~dataset.index.isin(ratio_list)]  

    ratio_selected_table['Review_Status'] = 'True_Match'
    ratio_unmatch_table['Review_Status']  = 'Manual_Match'    
    return ratio_selected_table,ratio_unmatch_table

def filter_wratio(dataset,col1=['Business Name','Name']):
    wratio_list=[]
    for j,k,l in zip(dataset[col1[0]].values, dataset[col1[1]].values, dataset[col1[0]].index):
        a3 = string_filter1(j.lower())      
        a4 = string_filter1(k.lower())
        
        scores = fuzz.WRatio(a3,a4)
        if scores>=90:#82
            #print(scores,l)
            wratio_list.append(l) 
            
    wratio_selected_table = dataset[dataset.index.isin(wratio_list)]
    wratio_unmatch_table = dataset[~dataset.index.isin(wratio_list)]

    wratio_selected_table['Review_Status'] = 'True_Match'
    wratio_unmatch_table['Review_Status']  = 'Manual_Match'     
    return wratio_selected_table,wratio_unmatch_table

def filter_token_sort_ratio(dataset,col1=['Business Name','Name']):
    token_sort_ratio_list=[]
    
    for j,k,l in zip(dataset[col1[0]].values,dataset[col1[1]].values,dataset[col1[0]].index):
        
        string1 = string_filter1(j.lower())       
        string2 = string_filter1(k.lower())
        
        scores = fuzz.token_sort_ratio(string1,string2)
        if scores>=90:
            #print(scores,l)
            token_sort_ratio_list.append(l) 
            
    token_sort_ratio_selected_table = dataset[dataset.index.isin(token_sort_ratio_list)]
    token_sort_ratio_unmatch_table  = dataset[~dataset.index.isin(token_sort_ratio_list)]  

    token_sort_ratio_selected_table['Review_Status'] = 'True_Match'
    token_sort_ratio_unmatch_table['Review_Status']  = 'Manual_Match'

    return token_sort_ratio_selected_table,token_sort_ratio_unmatch_table

def cross_check_results(dataset,col1,filename):
    print("*********** Filter Method Start ***************\n")
    print("******* Filter1-Methods (Process ExtractOne) ********")
    
    updated_records,unselected_records = filter_ratio_method(dataset=dataset,
                                                             col1=col1)
    print(updated_records.Review_Status.value_counts(),'\n')
    #----------------------------------------------------------------------------------------------------------------#
    print("********* Filter2-Methods (Partial Ratio) **********")

    partial_ratio_selected_table, partial_ratio_unmatch_table = filter_partial_ratio(dataset=unselected_records,
                                                                                    col1=col1)
    print(partial_ratio_selected_table.Review_Status.value_counts(),'\n')
    #----------------------------------------------------------------------------------------------------------------#
    print("********* Filter3-Methods (Token Sort Ratio) **********")
    
    token_sort_ratio_selected_table, token_sort_ratio_unmatch_table = filter_token_sort_ratio(dataset=partial_ratio_unmatch_table,
                                                                                                      col1=col1) 
    print(token_sort_ratio_selected_table.Review_Status.value_counts(),'\n')
    #----------------------------------------------------------------------------------------------------------------#
    print("********* Filter4-Methods (Ratio) **********")
    ratio_selected_table,ratio_unmatch_table = filter_ratio(dataset=token_sort_ratio_unmatch_table,
                                                            col1=col1)
    
    print(ratio_selected_table.Review_Status.value_counts(),'\n')
    #----------------------------------------------------------------------------------------------------------------#
    print("*********** Filter5-Methods (wratio) ***************")
    wratio_selected_table,wratio_unmatch_table = filter_wratio(dataset=ratio_unmatch_table, 
                                                               col1=col1)

    print(wratio_selected_table.Review_Status.value_counts(),'\n') # filter_partial_ratio_methods
    #----------------------------------------------------------------------------------------------------------------#
    print("*********** Filter6-Methods (Process ExtractOne) ***************")
    updated_partial_ratio_selected_table, updated_partial_ratio_unmatch_table = filter_partial_ratio_methods(dataset=wratio_unmatch_table,
                                                                                                             col1=col1)
    
    print(updated_partial_ratio_selected_table.Review_Status.value_counts(),'\n')
    print("*********** Filter Method Stop ***************")
    #----------------------------------------------------------------------------------------------------------------#
    total_matches = pd.concat([updated_records,
                               partial_ratio_selected_table,
                               token_sort_ratio_selected_table,
                               ratio_selected_table,
                               wratio_selected_table,
                               updated_partial_ratio_selected_table]) 

    print(total_matches.shape)
    
    complete_data = pd.concat([total_matches,updated_partial_ratio_unmatch_table])
    
    with pd.ExcelWriter(os.path.join(filename,'Exact_Matched_Records.xlsx')) as writer:  
         total_matches.to_excel(writer,sheet_name='Total Match Records',index=False)
            
            
    with pd.ExcelWriter(os.path.join(filename,'Manualy_Check_Matched_Records.xlsx')) as writer:  
         updated_partial_ratio_unmatch_table.to_excel(writer, sheet_name='Unselected Records',index=False)

    with pd.ExcelWriter(os.path.join(filename,'Complete_Fuzzy_Matched_Records.xlsx')) as writer:  
         complete_data.to_excel(writer, sheet_name='complete_data',index=False)
            
    return total_matches,updated_partial_ratio_unmatch_table
