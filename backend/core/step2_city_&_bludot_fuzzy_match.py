import os
import pandas as pd
from city_details import *
from src.final_name_matching import cross_check_results
from src.fuzzy_matching import fuzzy_based_string_matching

new_city_path = os.path.join(os.getcwd(),'cities_and_counties', CITY_NAME)

city_records   = pd.read_excel(os.path.join(new_city_path,'results','city_data','de_duplication_merged.xlsx'))
bludot_records = pd.read_excel(os.path.join(new_city_path,'results','bludot_data','bludot_concatenated_records.xlsx'))

print(city_records.columns)
if not os.path.exists(os.path.join(new_city_path,'results','auto_matches')):
        os.mkdir(os.path.join(new_city_path,'results','auto_matches'))

if not os.path.exists(os.path.join(new_city_path,'results','auto_matches_concated')):
        os.mkdir(os.path.join(new_city_path,'results','auto_matches_concated'))

def separate_main_spreadsheet(original_data, city_dataset, bludot_dataset):

    additional_city_dataset = city_dataset[~city_dataset.city_index.isin(original_data['city_index'].values)]
    additional_bludot_dataset = bludot_dataset[~bludot_dataset.bludot_index.isin(original_data['bludot_index'].values)]

    return additional_city_dataset, additional_bludot_dataset

def cartesian_list(l):
    l1=[]
    if len(l)==4:
        for i in range(len(l[1])):  
            for j in range(len(l[3])):
                l2=[]
                l2=[l[0],l[1][i],l[2]]
                l2.append(l[3][j])
                l1.append(l2)
    elif len(l)==2:
        for i in range(len(l[1])): 
            l2=[] 
            l2=[l[0],l[1][i]]
            l1.append(l2)
    # print(l1)
    return l1

def name_address_matching(name_address_list,mode,rule):
    main_list=cartesian_list(name_address_list)
    # print(main_list)
    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1')):
        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1'))
    if mode=='auto':
        iteration = 1
        for iter in range(0,2):
            for rules in main_list:
                print("Executed Rules:",rules,'\n')
                if iteration==1:
                    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}'))
                    
                    file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}')
                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    print('c',city_records_cols,'\n  b',bludot_records_cols)
                    print(city_records.columns)
                    auto_matched_records = fuzzy_based_string_matching(city_records,
                                                                        bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')#multiple,single
                
                if iteration>1:
                    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}'))
                    
                    #print(os.path.join(new_city_path,'results','auto_matches',f'Auto_Matches_{iteration}'),'<---------------------->')
                    file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration}')
                    updated_city_records= pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration-1}','Additional_City_Records.xlsx'))
                    updated_bludot_records = pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1',f'Auto_Matches_{iteration-1}','Additional_Bludot_Records.xlsx'))

                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    #print(city_records_cols,'\n',bludot_records_cols)
                    auto_matched_records = fuzzy_based_string_matching(updated_city_records,
                                                                        updated_bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')
                iteration+=1
    if mode=='manual':
        if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1')):
            os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1'))
        iteration = 1
        for iter in range(0,2):
            for rules in main_list:
                print("Executed Rules:",rules,'\n')
                if iteration==1:
                    if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}'))
                    
                    file_path           = os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}')
                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    print('c',city_records_cols,'\n  b',bludot_records_cols)
                    
                    auto_matched_records = fuzzy_based_string_matching(city_records,
                                                                        bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')#multiple,single
                
                if iteration>1:
                    if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}'))
                    
                    #print(os.path.join(new_city_path,'results','auto_matches',f'Auto_Matches_{iteration}'),'<---------------------->')
                    file_path           = os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration}')
                    updated_city_records= pd.read_excel(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration-1}','Additional_City_Records.xlsx'))
                    updated_bludot_records = pd.read_excel(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1',f'Manual_Matches_{iteration-1}','Additional_Bludot_Records.xlsx'))

                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    #print(city_records_cols,'\n',bludot_records_cols)
                    auto_matched_records = fuzzy_based_string_matching(updated_city_records,
                                                                        updated_bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')
                iteration+=1
    


def name_email_matching(name_email_list,mode,rule):
    main_list1=cartesian_list(name_email_list)
    # print(main_list1)
    

def name_phone_matching(name_phone_list,mode,rule):
    main_list2=cartesian_list(name_phone_list)
    # print(main_list2)
    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3')):
        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3'))
    iteration = 1
    for iter in range(0,2):
        for rules in main_list2:
            print("Executed Rules:",rules,'\n')
            if iteration==1:
                if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}')):
                    os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}'))
                
                file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}')
                city_records_cols   = [rules[1],rules[3]]
                bludot_records_cols = [rules[0],rules[2]]
                print('c',city_records_cols,'\n  b',bludot_records_cols)
                
                auto_matched_records = fuzzy_based_string_matching(city_records,
                                                                    bludot_records,
                                                                    city_records_cols,
                                                                    bludot_records_cols,
                                                                    file_path,
                                                                    mode,
                                                                    rule,
                                                                    runs='single')#multiple,single
            
            if iteration>1:
                if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}')):
                    os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}'))
                
                #print(os.path.join(new_city_path,'results','auto_matches',f'Auto_Matches_{iteration}'),'<---------------------->')
                file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration}')
                updated_city_records= pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration-1}','Additional_City_Records.xlsx'))
                updated_bludot_records = pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3',f'Auto_Matches_{iteration-1}','Additional_Bludot_Records.xlsx'))

                city_records_cols   = [rules[1],rules[3]]
                bludot_records_cols = [rules[0],rules[2]]
                #print(city_records_cols,'\n',bludot_records_cols)
                auto_matched_records = fuzzy_based_string_matching(updated_city_records,
                                                                    updated_bludot_records,
                                                                    city_records_cols,
                                                                    bludot_records_cols,
                                                                    file_path,
                                                                    mode,
                                                                    rule,
                                                                    runs='single')
            iteration+=1

def name_matching(name_list,mode,rule):
    main_list=cartesian_list(name_list)
    # print(main_list)
    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5')):
        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5'))
    if mode=='auto':
        iteration = 1
        for iter in range(0,2):
            for rules in main_list:
                print("Executed Rules:",rules,'\n')
                if iteration==1:
                    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}'))
                    
                    file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}')
                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    print('c',city_records_cols,'\n  b',bludot_records_cols)
                    
                    auto_matched_records = fuzzy_based_string_matching(city_records,
                                                                        bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')#multiple,single
                
                if iteration>1:
                    if not os.path.exists(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}'))
                    
                    #print(os.path.join(new_city_path,'results','auto_matches',f'Auto_Matches_{iteration}'),'<---------------------->')
                    file_path           = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration}')
                    updated_city_records= pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration-1}','Additional_City_Records.xlsx'))
                    updated_bludot_records = pd.read_excel(os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5',f'Auto_Matches_{iteration-1}','Additional_Bludot_Records.xlsx'))

                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    #print(city_records_cols,'\n',bludot_records_cols)
                    auto_matched_records = fuzzy_based_string_matching(updated_city_records,
                                                                        updated_bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')
                iteration+=1
    if mode=='manual':
        if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5')):
            os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5'))
        iteration = 1
        for iter in range(0,2):
            for rules in main_list:
                print("Executed Rules:",rules,'\n')
                if iteration==1:
                    if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}'))
                    
                    file_path           = os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}')
                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    print('c',city_records_cols,'\n  b',bludot_records_cols)
                    
                    auto_matched_records = fuzzy_based_string_matching(city_records,
                                                                        bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')#multiple,single
                
                if iteration>1:
                    if not os.path.exists(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}')):
                        os.mkdir(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}'))
                    
                    #print(os.path.join(new_city_path,'results','auto_matches',f'Auto_Matches_{iteration}'),'<---------------------->')
                    file_path           = os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration}')
                    updated_city_records= pd.read_excel(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration-1}','Additional_City_Records.xlsx'))
                    updated_bludot_records = pd.read_excel(os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule5',f'Manual_Matches_{iteration-1}','Additional_Bludot_Records.xlsx'))

                    city_records_cols   = [rules[1],rules[3]]
                    bludot_records_cols = [rules[0],rules[2]]
                    #print(city_records_cols,'\n',bludot_records_cols)
                    auto_matched_records = fuzzy_based_string_matching(updated_city_records,
                                                                        updated_bludot_records,
                                                                        city_records_cols,
                                                                        bludot_records_cols,
                                                                        file_path,
                                                                        mode,
                                                                        rule,
                                                                        runs='single')
                iteration+=1

def lat_long_matching(lat_long_list,mode,rule):
    main_list3=cartesian_list(lat_long_list)
    # print(main_list3)


            
for rules in AUTO_RULE_LIST:
    if rules==RULE1:
        name_address_matching(RULE1,'auto','rule1')
        work_dir_loc = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule1')

        list_sub_folder = []
        for sub_path in os.listdir(work_dir_loc):
            #print(sub_path)
            list_sub_folder.append(sub_path)

        #Adding File Details  
        updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,'Total_Matched_Records.xlsx')) for sub_file in list_sub_folder])
        if not os.path.exists(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule1')):
            os.mkdir(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule1'))
        file_loc_path = os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule1') #Beautiful_Excel,Final-Decision


        additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                                    city_dataset=city_records,
                                                                                    bludot_dataset=bludot_records)
        
        updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)
                                    
        additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        city_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'))
        bludot_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'))

    if rules==RULE2:
        name_email_matching(RULE2,'auto','rule2')
    
    elif rules==RULE3:
        name_phone_matching(RULE3,'auto','rule3')
        work_dir_loc = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule3')

        list_sub_folder = []
        for sub_path in os.listdir(work_dir_loc):
            #print(sub_path)
            list_sub_folder.append(sub_path)

        #Adding File Details  
        updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,'Total_Matched_Records.xlsx')) for sub_file in list_sub_folder])
        if not os.path.exists(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule3')):
            os.mkdir(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule3'))
        file_loc_path = os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule3') #Beautiful_Excel,Final-Decision


        additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                                    city_dataset=city_records,
                                                                                    bludot_dataset=bludot_records)
        
        updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)
                                    
        additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        city_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'))
        bludot_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'))
    
    elif rules==RULE4:
        lat_long_matching(RULE4,'auto','rule4')

    elif rules==RULE5:
        name_matching(RULE5,'auto','rule5')
        # print('entered')
        work_dir_loc = os.path.join(new_city_path,'results','auto_matches','auto_matches-Rule5')

        list_sub_folder = []
        for sub_path in os.listdir(work_dir_loc):
            #print(sub_path)
            list_sub_folder.append(sub_path)

        #Adding File Details  
        updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,'Total_Matched_Records.xlsx')) for sub_file in list_sub_folder])
        if not os.path.exists(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule5')):
            os.mkdir(os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule5'))
        file_loc_path = os.path.join(new_city_path,'results','auto_matches_concated','Final-Decision-Rule5') #Beautiful_Excel,Final-Decision


        additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                                    city_dataset=city_records,
                                                                                    bludot_dataset=bludot_records)
        
        updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)
                                    
        additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        city_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'))
        bludot_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'))


if not os.path.exists(os.path.join(new_city_path,'results','manual_matches')):
            os.mkdir(os.path.join(new_city_path,'results','manual_matches'))

for rules in MANUAL_RULE_LIST:
    if rules==RULE1:
        name_address_matching(RULE1,'manual','rule1')
        work_dir_loc = os.path.join(new_city_path,'results','manual_matches','manual_matches-Rule1')

        list_sub_folder = []
        for sub_path in os.listdir(work_dir_loc):
            #print(sub_path)
            list_sub_folder.append(sub_path)

        #Adding File Details  
        updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,'Total_Matched_Records.xlsx')) for sub_file in list_sub_folder])
        if not os.path.exists(os.path.join(new_city_path,'results','Final-Decision-Manual')):
            os.mkdir(os.path.join(new_city_path,'results','Final-Decision-Manual'))
        if not os.path.exists(os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule1')):
            os.mkdir(os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule1'))
        file_loc_path = os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule1') #Beautiful_Excel,Final-Decision


        additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                                    city_dataset=city_records,
                                                                                    bludot_dataset=bludot_records)
        
        updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)
                                    
        additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        city_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'))
        bludot_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'))

    
    if rules==RULE5:
        name_matching(RULE5,'manual','rule5')

        list_sub_folder = []
        for sub_path in os.listdir(work_dir_loc):
            #print(sub_path)
            list_sub_folder.append(sub_path)

        #Adding File Details  
        updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,'Total_Matched_Records.xlsx')) for sub_file in list_sub_folder])
        if not os.path.exists(os.path.join(new_city_path,'results','Final-Decision-Manual')):
            os.mkdir(os.path.join(new_city_path,'results','Final-Decision-Manual'))
        if not os.path.exists(os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule5')):
            os.mkdir(os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule5'))
        file_loc_path = os.path.join(new_city_path,'results','Final-Decision-Manual','Final-Decision-Manual-Rule5') #Beautiful_Excel,Final-Decision


        additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                                    city_dataset=city_records,
                                                                                    bludot_dataset=bludot_records)
        
        updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)
                                    
        additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                                    index=False)

        city_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'))
        bludot_records=pd.read_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'))
    
    elif rules==RULE2:
        name_email_matching(RULE2,'manual','rule2')
    elif rules==RULE3:
        name_phone_matching(RULE3,'manual','rule3')




if not os.path.exists(os.path.join(new_city_path,'results','final_auto_matches')):
            os.mkdir(os.path.join(new_city_path,'results','final_auto_matches'))

work_dir_loc = os.path.join(new_city_path,'results','auto_matches_concated')

list_sub_folder = []
for sub_path in os.listdir(work_dir_loc):
    #print(sub_path)
    list_sub_folder.append(sub_path)

#Adding File Details  
updated_excel_sheet = pd.concat([pd.read_excel(os.path.join(work_dir_loc,sub_file,f'Final_Matched_Records_for_{CITY_NAME}.xlsx')) for sub_file in list_sub_folder])

file_loc_path = os.path.join(new_city_path,'results','final_auto_matches') #Beautiful_Excel,Final-Decision


additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                              city_dataset=city_records,
                                                                              bludot_dataset=bludot_records)
 
updated_excel_sheet.to_excel(os.path.join(file_loc_path,f'Final_Matched_Records_for_{CITY_NAME}.xlsx'),
                             index=False)
                             
additional_city_dataset.to_excel(os.path.join(file_loc_path,f'Additional_City_Records_for_{CITY_NAME}.xlsx'),
                             index=False)

additional_bludot_dataset.to_excel(os.path.join(file_loc_path,f'Additional_Bludot_Records_for_{CITY_NAME}.xlsx'),
                             index=False)

#------------------------------------------------------------------------------------------------------------------------------------------------
print("Step5: Filter Out Matching Records Based on Fuzzy Scores")
#------------------------------------------------------------------------------------------------------------------------------------------------
filter_out_matching_records  = pd.read_excel(os.path.join(new_city_path,'results','final_auto_matches',f'Final_Matched_Records_for_{CITY_NAME}.xlsx'))
col1     = [CITY_NAME_LIST[0],BLUDOT_NAME]

if not os.path.exists(os.path.join(new_city_path,'results','filter_Matches')):
            os.mkdir(os.path.join(new_city_path,'results','filter_matches'))

filename = os.path.join(new_city_path,'results','filter_matches')

true_table,manual_table = cross_check_results(dataset=filter_out_matching_records,
                                              col1=col1,
                                              filename=filename)
print(true_table.shape,manual_table.shape)

# manual_excel_sheet=pd.read_excel(os.path.join(new_city_path,'Results','final Manual-Matches',f'Final_Matched_Records_for_{name}.xlsx'))

# manual_excel_sheet.to_excel(os.path.join(filename,'Manual_check_Matched_Records.xlsx'),index=False)