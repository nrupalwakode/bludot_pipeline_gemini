import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import difflib
from typing import Dict, List, Tuple, Set
import threading

def normalize_phone_number(phone_str):
    """Normalize phone number by removing country codes and punctuation"""
    if pd.isna(phone_str) or phone_str == "":
        return phone_str
    
    # Convert to string and clean
    phone = str(phone_str).strip()
    
    # Remove .0 if it's a float representation
    if phone.endswith('.0'):
        phone = phone[:-2]

    # Remove all non-digit characters
    digits_only = re.sub(r'[^\d]', '', phone)

    # Return original if no digits found or invalid
    if not digits_only or not digits_only.isdigit():
        return phone_str
    
    # Remove common country codes (1, 01, etc.) if phone is longer than 10 digits
    if len(digits_only) > 10:
        # Remove leading 1 for US numbers
        if digits_only.startswith('1') and len(digits_only) == 11:
            digits_only = digits_only[1:]
        # Remove leading 01 for some international formats
        elif digits_only.startswith('01') and len(digits_only) == 12:
            digits_only = digits_only[2:]
            
    # Return the processed digits_only for valid 10-digit numbers
    if len(digits_only) == 10 and digits_only.isdigit():
        return digits_only
    elif len(digits_only) < 10:
        # Return as-is if too short (might be intentional)
        return digits_only
    else:
        # Return original if too long or invalid after processing
        return phone_str

def names_are_similar(name1, name2, threshold=0.87):
    """Check if two names are similar using string similarity"""
    if pd.isna(name1) or pd.isna(name2) or name1 == "" or name2 == "":
        return False
    
    name1_clean = str(name1).strip().lower()
    name2_clean = str(name2).strip().lower()
    
    if name1_clean == name2_clean:
        return True
    
    # Check if one name is contained in another
    if name1_clean in name2_clean or name2_clean in name1_clean:
        return True
    
    # Use difflib for similarity scoring
    similarity = difflib.SequenceMatcher(None, name1_clean, name2_clean).ratio()
    return similarity >= threshold

def choose_better_name(name1, name2):
    """Choose the more complete/logical name between two similar names"""
    if pd.isna(name1) or name1 == "":
        return name2
    if pd.isna(name2) or name2 == "":
        return name1
    
    str1 = str(name1).strip()
    str2 = str(name2).strip()
    
    # Prefer longer, more complete names
    if len(str1.split()) > len(str2.split()):
        return str1
    elif len(str2.split()) > len(str1.split()):
        return str2
    else:
        # If same word count, prefer alphabetically first (more stable)
        return str1 if str1 <= str2 else str2

def merge_titles_or_roles(field1, field2, threshold=0.90):
    """Merge titles or roles based on similarity, avoiding duplicates"""
    if pd.isna(field1) or field1 == "":
        return field2 if pd.notna(field2) and field2 != "" else ""
    if pd.isna(field2) or field2 == "":
        return field1
    
    str1 = str(field1).strip()
    str2 = str(field2).strip()
    
    if str1.lower() == str2.lower():
        return str1
    
    # Split existing field1 by comma to get individual items
    existing_items = [item.strip() for item in str1.split(',') if item.strip()]
    new_item = str2.strip()
    
    # Check if new_item is similar to any existing item
    for existing_item in existing_items:
        similarity = difflib.SequenceMatcher(None, existing_item.lower(), new_item.lower()).ratio()
        if similarity >= threshold:
            # Replace with the longer/more complete version
            better_item = existing_item if len(existing_item) >= len(new_item) else new_item
            # Replace the existing item with the better one
            existing_items = [better_item if item == existing_item else item for item in existing_items]
            return ', '.join(existing_items)
    
    # If no similar item found, add the new item
    existing_items.append(new_item)
    return ', '.join(existing_items)

def contact_sets_equal(contact1, contact_type1, type1, contact2, contact_type2, type2, ignore_others_type=False, normalized1=None, normalized2=None):
    """Check if two contact sets are equal (case insensitive)"""
    def normalize_val(val):
        return str(val).strip().lower() if pd.notna(val) and val != "" else ""
    
    # Use normalized versions if provided, otherwise use original
    contact1_to_compare = normalized1 if normalized1 is not None else contact1
    contact2_to_compare = normalized2 if normalized2 is not None else contact2
    
    contact_match = normalize_val(contact1_to_compare) == normalize_val(contact2_to_compare)
    contact_type_match = normalize_val(contact_type1) == normalize_val(contact_type2)
    
    if ignore_others_type:
        return contact_match and contact_type_match
    else:
        type_match = normalize_val(type1) == normalize_val(type2)
        return contact_match and contact_type_match and type_match

def should_remove_others_contact(contact_set, other_contact_sets):
    """Check if a contact set with 'others'/'other' type should be removed"""
    if pd.isna(contact_set['type']) or str(contact_set['type']).strip().lower() not in ['others', 'other']:
        return False
    
    # Check if there's a matching contact set with different type
    for other_set in other_contact_sets:
        if (contact_sets_equal(
            contact_set['contact'], contact_set['contact_type'], contact_set['type'],
            other_set['contact'], other_set['contact_type'], other_set['type'],
            ignore_others_type=True,
            normalized1=contact_set.get('contact_normalized'),
            normalized2=other_set.get('contact_normalized')
        ) and str(other_set['type']).strip().lower() not in ['others', 'other']):
            return True
    
    return False

def email_matches_name(email, name):
    """Check if email is similar to contact name"""
    if pd.isna(email) or pd.isna(name) or email == "" or name == "":
        return False
    
    email_str = str(email).lower().strip()
    name_str = str(name).lower().strip()
    
    # Extract name part from email (before @)
    email_name = email_str.split('@')[0] if '@' in email_str else email_str
    
    # Remove common separators and check similarity
    email_name_clean = re.sub(r'[._\-]', '', email_name)
    name_clean = re.sub(r'[._\-\s]', '', name_str)
    
    return email_name_clean in name_clean or name_clean in email_name_clean

def process_single_row(row_data):
    """Process a single row of contact data"""
    try:
        row = row_data.copy()
        
        # Extract all contact information
        contacts = []
        max_contacts = 200  # Reasonable upper bound
        
        # First, collect the primary contact
        if 'Name' in row and pd.notna(row.get('Name')) and str(row.get('Name')).strip():
            primary_contact = {
                'name': str(row.get('Name', '')).strip(),
                'title': str(row.get('Title', '')).strip() if pd.notna(row.get('Title')) else '',
                'roles': str(row.get('Roles', '')).strip() if pd.notna(row.get('Roles')) else '',
                'contact_sets': []
            }
            
            # Add primary contact set if exists
            if pd.notna(row.get('Contact')) and str(row.get('Contact')).strip():
                contact_val = str(row['Contact']).strip()
                contact_type = str(row.get('Contact_type', '')).strip() if pd.notna(row.get('Contact_type')) else ''
                type_val = str(row.get('Type', '')).strip() if pd.notna(row.get('Type')) else ''
                
                # Store original for output, but add normalized version for comparison
                original_contact = contact_val
                normalized_contact = normalize_phone_number(contact_val) if contact_type.lower() == 'phone_number' else contact_val
                
                primary_contact['contact_sets'].append({
                    'contact': original_contact,  # Keep original for output
                    'contact_normalized': normalized_contact,  # Add normalized for comparison
                    'contact_type': contact_type,
                    'type': type_val
                })
            
            contacts.append(primary_contact)
        
        # Collect numbered contacts
        for i in range(2, max_contacts):
            name_key = f'{i}_Name'
            if name_key not in row:
                break
                
            name_val = row.get(name_key)
            if pd.isna(name_val) or str(name_val).strip() == '':
                continue
                
            contact_info = {
                'name': str(name_val).strip(),
                'title': str(row.get(f'{i}_Title', '')).strip() if pd.notna(row.get(f'{i}_Title')) else '',
                'roles': str(row.get(f'{i}_Roles', '')).strip() if pd.notna(row.get(f'{i}_Roles')) else '',
                'contact_sets': []
            }
            
            # Collect all contact sets for this name
            contact_idx = 0
            while True:
                if contact_idx == 0:
                    contact_key = f'{i}_Contact'
                    contact_type_key = f'{i}_Contact_type'
                    type_key = f'{i}_Type'
                else:
                    # This would be for additional contact sets, but based on the data structure
                    # it seems each numbered entry has only one contact set
                    break
                
                if contact_key not in row:
                    break
                    
                contact_val = row.get(contact_key)
                if pd.isna(contact_val) or str(contact_val).strip() == '':
                    break
                    
                contact_type = str(row.get(contact_type_key, '')).strip() if pd.notna(row.get(contact_type_key)) else ''
                type_val = str(row.get(type_key, '')).strip() if pd.notna(row.get(type_key)) else ''

                # Store original for output, but add normalized version for comparison
                original_contact = str(contact_val).strip()
                normalized_contact = normalize_phone_number(original_contact) if contact_type.lower() == 'phone_number' else original_contact

                contact_info['contact_sets'].append({
                    'contact': original_contact,  # Keep original for output
                    'contact_normalized': normalized_contact,  # Add normalized for comparison
                    'contact_type': contact_type,
                    'type': type_val
                })
                
                contact_idx += 1
            
            contacts.append(contact_info)
        
        # Process deduplication and reassignment
        processed_contacts = []
        
        # Step 1: Handle similar name deduplication
        i = 0
        while i < len(contacts):
            current_contact = contacts[i]
            merged = False
            
            for j in range(len(processed_contacts)):
                if (current_contact['name'] != '-' and processed_contacts[j]['name'] != '-' and 
                    names_are_similar(current_contact['name'], processed_contacts[j]['name'])):
                    
                    # Choose better name
                    better_name = choose_better_name(current_contact['name'], processed_contacts[j]['name'])
                    processed_contacts[j]['name'] = better_name
                    
                    # Merge titles and roles
                    processed_contacts[j]['title'] = merge_titles_or_roles(
                        processed_contacts[j]['title'], current_contact['title']
                    )
                    processed_contacts[j]['roles'] = merge_titles_or_roles(
                        processed_contacts[j]['roles'], current_contact['roles']
                    )

                    # Merge contact sets, avoiding duplicates
                    for contact_set in current_contact['contact_sets']:
                        
                        # Check if this contact set should be removed due to others/other type
                        if should_remove_others_contact(contact_set, processed_contacts[j]['contact_sets']):
                            continue

                        is_duplicate = False
                        for existing_set in processed_contacts[j]['contact_sets']:
                            if contact_sets_equal(
                                contact_set['contact'], contact_set['contact_type'], contact_set['type'],
                                existing_set['contact'], existing_set['contact_type'], existing_set['type'],
                                ignore_others_type=False,
                                normalized1=contact_set.get('contact_normalized'),
                                normalized2=existing_set.get('contact_normalized')
                            ):
                                is_duplicate = True
                                break
                        
                        if not is_duplicate:
                            processed_contacts[j]['contact_sets'].append(contact_set)
                    
                    # Also remove others/other from existing sets if there are duplicates
                    processed_contacts[j]['contact_sets'] = [
                        cs for cs in processed_contacts[j]['contact_sets'] 
                        if not should_remove_others_contact(cs, processed_contacts[j]['contact_sets'])
                    ]
                    
                    merged = True
                    break
            
            if not merged:
                processed_contacts.append(current_contact)
            
            i += 1
        
        # Step 2: Handle BLK contacts
        blk_contacts = [c for c in processed_contacts if c['name'].startswith('BLK_')]
        non_blk_contacts = [c for c in processed_contacts if not c['name'].startswith('BLK_')]

        # Handle BLK contact deduplication
        for i, blk_contact in enumerate(blk_contacts):
            for contact_set in blk_contact['contact_sets'][:]:  # Use slice to avoid modification during iteration
                removed = False
                
                # Check against other BLK contacts
                for j, other_blk in enumerate(blk_contacts):
                    if i != j:
                        for other_set in other_blk['contact_sets'][:]:
                            # Rule 1: Completely same contact sets
                            if contact_sets_equal(
                                contact_set['contact'], contact_set['contact_type'], contact_set['type'],
                                other_set['contact'], other_set['contact_type'], other_set['type'],
                                ignore_others_type=False,
                                normalized1=contact_set.get('contact_normalized'),
                                normalized2=other_set.get('contact_normalized')
                            ):
                                blk_contact['contact_sets'].remove(contact_set)
                                removed = True
                                break
                            
                            # Rule 2: Same contact and contact_type, one has "other"/"others" type
                            if (str(contact_set['contact']).strip().lower() == str(other_set['contact']).strip().lower() and
                                str(contact_set['contact_type']).strip().lower() == str(other_set['contact_type']).strip().lower()):
                                
                                contact_type_lower = str(contact_set['type']).strip().lower()
                                other_type_lower = str(other_set['type']).strip().lower()
                                
                                if contact_type_lower in ['other', 'others'] and other_type_lower not in ['other', 'others']:
                                    blk_contact['contact_sets'].remove(contact_set)
                                    removed = True
                                    break
                                elif other_type_lower in ['other', 'others'] and contact_type_lower not in ['other', 'others']:
                                    other_blk['contact_sets'].remove(other_set)
                                    break
                        
                        if removed:
                            break
                    if removed:
                        break
                
                if removed:
                    continue
                    
                # Check against non-BLK contacts
                for non_blk in non_blk_contacts:
                    for existing_set in non_blk['contact_sets'][:]:
                        # Rule 3: Completely same contact sets - remove from BLK
                        if contact_sets_equal(
                            contact_set['contact'], contact_set['contact_type'], contact_set['type'],
                            existing_set['contact'], existing_set['contact_type'], existing_set['type'],
                            ignore_others_type=False,
                            normalized1=contact_set.get('contact_normalized'),
                            normalized2=existing_set.get('contact_normalized')
                        ):
                            blk_contact['contact_sets'].remove(contact_set)
                            removed = True
                            break
                        
                        # Rule 4: Same contact and contact_type, one has "other"/"others" type
                        if (str(contact_set['contact']).strip().lower() == str(existing_set['contact']).strip().lower() and
                            str(contact_set['contact_type']).strip().lower() == str(existing_set['contact_type']).strip().lower()):
                            
                            blk_type_lower = str(contact_set['type']).strip().lower()
                            existing_type_lower = str(existing_set['type']).strip().lower()
                            
                            if blk_type_lower in ['other', 'others'] and existing_type_lower not in ['other', 'others']:
                                # Remove BLK contact set with "other" type
                                blk_contact['contact_sets'].remove(contact_set)
                                removed = True
                                break
                            elif existing_type_lower in ['other', 'others'] and blk_type_lower not in ['other', 'others']:
                                # Remove existing contact set with "other" type and add BLK contact set to non-BLK
                                non_blk['contact_sets'].remove(existing_set)
                                non_blk['contact_sets'].append(contact_set)
                                blk_contact['contact_sets'].remove(contact_set)
                                removed = True
                                break
                    
                    if removed:
                        break

        # Step 3: Merge duplicate BLK contacts (same as dash logic but for BLK)
        # blk_contacts = [c for c in processed_contacts if c['name'].startswith('BLK_')]
        # if len(blk_contacts) > 1:
        #     # Merge all BLK contacts into the first one
        #     primary_blk = blk_contacts[0]
        #     for i in range(1, len(blk_contacts)):
        #         for contact_set in blk_contacts[i]['contact_sets']:
        #             is_duplicate = False
        #             for existing_set in primary_blk['contact_sets']:
        #                 if contact_sets_equal(
        #                     contact_set['contact'], contact_set['contact_type'], contact_set['type'],
        #                     existing_set['contact'], existing_set['contact_type'], existing_set['type'],
        #                     ignore_others_type=False
        #                 ):
        #                     is_duplicate = True
        #                     break
                    
        #             if not is_duplicate:
        #                 primary_blk['contact_sets'].append(contact_set)
                
        #         processed_contacts.remove(blk_contacts[i])
        
        # Step 4: Clean up empty contacts
        processed_contacts = [
            c for c in processed_contacts 
            if c['contact_sets'] and any(
                c_set['contact'] or c_set['contact_type'] or c_set['type'] 
                for c_set in c['contact_sets']
            ) or not c['name'].startswith('BLK_')
        ]
        
        # Step 5: Final validation checks
        for contact in processed_contacts:
            # Check 1: Ensure each name has at least one contact set
            if not contact['contact_sets'] and contact['name'] != '-':
                # Add an empty contact set
                contact['contact_sets'].append({
                    'contact': '',
                    'contact_type': '',
                    'type': ''
                })
            
            # Check 2: Handle empty names (should not happen with BLK system)
            if not contact['name'] or contact['name'].strip() == '':
                contact['name'] = 'BLK_UNKNOWN'
            
            # Check 3: Remove duplicate contact sets (case insensitive)
            unique_sets = []
            seen_sets = set()
            
            for contact_set in contact['contact_sets']:
                # Create a normalized tuple for comparison
                normalized_key = (
                    str(contact_set['contact']).strip().lower(),
                    str(contact_set['contact_type']).strip().lower(),
                    str(contact_set['type']).strip().lower()
                )
                
                if normalized_key not in seen_sets:
                    seen_sets.add(normalized_key)
                    unique_sets.append(contact_set)
            
            contact['contact_sets'] = unique_sets

        # Additional check: Ensure contacts with only empty contact sets are handled
        processed_contacts = [
            c for c in processed_contacts 
            if c['contact_sets'] and any(
                c_set['contact'] or c_set['contact_type'] or c_set['type'] 
                for c_set in c['contact_sets']
            ) or c['name'] != '-'
        ]    

        # Step 6: Assign "-" to orphaned contact sets (shouldn't happen based on structure)
        
        # Build output row with proper column ordering
        output_row = {}
        column_order = []
        
        # Start with ID
        output_row['ID'] = row.get('ID', '')
        column_order.append('ID')
        
        if processed_contacts:
            # First contact
            first_contact = processed_contacts[0]
            output_row['Name'] = first_contact['name']
            output_row['Title'] = first_contact['title']
            output_row['Roles'] = first_contact['roles']
            column_order.extend(['Name', 'Title', 'Roles'])
            
            # Add contact sets for first contact
            contact_sets = first_contact['contact_sets']
            for i, contact_set in enumerate(contact_sets):
                if i == 0:
                    output_row['Contact'] = contact_set['contact']
                    output_row['Contact_type'] = contact_set['contact_type']
                    output_row['Type'] = contact_set['type']
                    column_order.extend(['Contact', 'Contact_type', 'Type'])
                else:
                    contact_col = f'Contact.{i}'
                    contact_type_col = f'Contact_type.{i}'
                    type_col = f'Type.{i}'
                    output_row[contact_col] = contact_set['contact']
                    output_row[contact_type_col] = contact_set['contact_type']
                    output_row[type_col] = contact_set['type']
                    column_order.extend([contact_col, contact_type_col, type_col])
            
            # Additional contacts
            for contact_idx, contact_info in enumerate(processed_contacts[1:], 1):
                base_idx = contact_idx + 1
                
                # Add name, title, roles columns
                name_col = f'{base_idx}_Name'
                title_col = f'{base_idx}_Title'
                roles_col = f'{base_idx}_Roles'
                
                output_row[name_col] = contact_info['name']
                output_row[title_col] = contact_info['title']
                output_row[roles_col] = contact_info['roles']
                column_order.extend([name_col, title_col, roles_col])
                
                # Add contact sets for this contact
                for set_idx, contact_set in enumerate(contact_info['contact_sets']):
                    if set_idx == 0:
                        contact_col = f'{base_idx}_Contact'
                        contact_type_col = f'{base_idx}_Contact_type'
                        type_col = f'{base_idx}_Type'
                    else:
                        contact_col = f'{base_idx}_Contact.{set_idx}'
                        contact_type_col = f'{base_idx}_Contact_type.{set_idx}'
                        type_col = f'{base_idx}_Type.{set_idx}'
                    
                    output_row[contact_col] = contact_set['contact']
                    output_row[contact_type_col] = contact_set['contact_type']
                    output_row[type_col] = contact_set['type']
                    column_order.extend([contact_col, contact_type_col, type_col])
        
        return output_row
        
    except Exception as e:
        print(f"Error processing row: {e}")
        return row_data

def clean_column_names(df):
    def clean_name(col):
        # Remove prefixes like 1_, 2_, 1_2_, etc.
        col = re.sub(r'^(\d+_)+', '', col)
        # Remove suffixes like .1, .2, etc.
        col = re.sub(r'\.\d+$', '', col)
        return col

    df.columns = [clean_name(col) for col in df.columns]
    return df

def format_contact_data(df, max_workers=None):
    """
    Main function to format contact data with multithreading
    
    Args:
        df: pandas DataFrame with contact data
        max_workers: Number of threads to use (None for auto)
    
    Returns:
        pandas DataFrame with formatted contact data
    """

    print(str(df.head(1)))
    # df = df.astype(str)
    # print(str(df.head(1)))

    if max_workers is None:
        max_workers = max(min(32, len(df)),1)
    
    # Convert DataFrame to list of dictionaries for processing
    rows_data = df.to_dict('records')
    
    # Process rows with multithreading
    processed_rows = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_row = {executor.submit(process_single_row, row): i for i, row in enumerate(rows_data)}
        
        # Collect results with progress bar
        for future in tqdm(as_completed(future_to_row), total=len(rows_data), desc="Processing contacts"):
            try:
                result = future.result()
                row_index = future_to_row[future]
                processed_rows.append((row_index, result))
            except Exception as e:
                row_index = future_to_row[future]
                print(f"Error processing row {row_index}: {e}")
                processed_rows.append((row_index, rows_data[row_index]))
    
    # Sort by original order and extract results
    processed_rows.sort(key=lambda x: x[0])
    result_data = [row[1] for row in processed_rows]
    
    # Convert back to DataFrame with proper column ordering
    result_df = pd.DataFrame(result_data)
    
    # Ensure proper column ordering
    if not result_df.empty:
        # Get all columns and sort them properly
        columns = list(result_df.columns)
        ordered_columns = ['ID'] if 'ID' in columns else []
        
        # Separate into different types of columns
        name_cols = []
        title_cols = []
        roles_cols = []
        contact_cols = []
        contact_type_cols = []
        type_cols = []
        
        for col in columns:
            if col == 'ID':
                continue
            elif col == 'Name' or col.endswith('_Name'):
                name_cols.append(col)
            elif col == 'Title' or col.endswith('_Title'):
                title_cols.append(col)
            elif col == 'Roles' or col.endswith('_Roles'):
                roles_cols.append(col)
            elif col == 'Contact' or col.endswith('_Contact') or 'Contact.' in col:
                contact_cols.append(col)
            elif col == 'Contact_type' or col.endswith('_Contact_type') or 'Contact_type.' in col:
                contact_type_cols.append(col)
            elif col == 'Type' or col.endswith('_Type') or 'Type.' in col:
                type_cols.append(col)
        
        # Sort each type of column properly
        def sort_key(col):
            if col in ['Name', 'Title', 'Roles', 'Contact', 'Contact_type', 'Type']:
                return (0, col)
            elif '.' in col:
                # Handle Contact.1, Contact_type.1, etc.
                parts = col.split('.')
                if len(parts) == 2:
                    base, num = parts
                    return (0, base, int(num))
            elif '_' in col:
                # Handle 2_Name, 2_Contact, etc.
                parts = col.split('_', 1)
                if parts[0].isdigit():
                    return (int(parts[0]), parts[1])
            return (999, col)
        
        name_cols.sort(key=sort_key)
        title_cols.sort(key=sort_key)
        roles_cols.sort(key=sort_key)
        contact_cols.sort(key=sort_key)
        contact_type_cols.sort(key=sort_key)
        type_cols.sort(key=sort_key)
        
        # Build the final column order
        # First handle the primary contact (no number prefix)
        if 'Name' in name_cols:
            ordered_columns.extend(['Name', 'Title', 'Roles'])
            # Add primary contact sets
            primary_contacts = [col for col in contact_cols if not col.startswith(tuple('123456789'))]
            primary_contact_types = [col for col in contact_type_cols if not col.startswith(tuple('123456789'))]
            primary_types = [col for col in type_cols if not col.startswith(tuple('123456789'))]
            
            primary_contacts.sort(key=sort_key)
            primary_contact_types.sort(key=sort_key)
            primary_types.sort(key=sort_key)
            
            # Interleave contact sets properly
            max_primary = max(len(primary_contacts), len(primary_contact_types), len(primary_types))
            for i in range(max_primary):
                if i < len(primary_contacts):
                    ordered_columns.append(primary_contacts[i])
                if i < len(primary_contact_types):
                    ordered_columns.append(primary_contact_types[i])
                if i < len(primary_types):
                    ordered_columns.append(primary_types[i])
        
        # Then handle numbered contacts in order
        contact_numbers = set()
        for col in name_cols + title_cols + roles_cols + contact_cols + contact_type_cols + type_cols:
            if '_' in col:
                parts = col.split('_', 1)
                if parts[0].isdigit():
                    contact_numbers.add(int(parts[0]))
        
        for num in sorted(contact_numbers):
            # Add name, title, roles for this number
            for col in [f'{num}_Name', f'{num}_Title', f'{num}_Roles']:
                if col in columns:
                    ordered_columns.append(col)
            
            # Add contact sets for this number
            num_contacts = [col for col in contact_cols if col.startswith(f'{num}_')]
            num_contact_types = [col for col in contact_type_cols if col.startswith(f'{num}_')]
            num_types = [col for col in type_cols if col.startswith(f'{num}_')]
            
            num_contacts.sort(key=sort_key)
            num_contact_types.sort(key=sort_key)
            num_types.sort(key=sort_key)
            
            # Interleave contact sets properly
            max_num = max(len(num_contacts), len(num_contact_types), len(num_types))
            for i in range(max_num):
                if i < len(num_contacts):
                    ordered_columns.append(num_contacts[i])
                if i < len(num_contact_types):
                    ordered_columns.append(num_contact_types[i])
                if i < len(num_types):
                    ordered_columns.append(num_types[i])
        
        # Reorder the DataFrame columns
        available_columns = [col for col in ordered_columns if col in result_df.columns]
        result_df = result_df[available_columns]
    # result_df = clean_column_names(df)
    return result_df

# Example usage function (for testing)
def main():
    """Example usage of the contact formatter"""
    # This would be called by your main code
    df = pd.read_csv(r"C:\Users\USER\Documents\Backup\Backup\new_city_onboarding\cities_and_counties\Long_Beach_26_06_2025\results\contact_matched_intermediate.csv")
    formatted_df = format_contact_data(df)
    formatted_df.to_csv(r"C:\Users\USER\Documents\Backup\Backup\new_city_onboarding\cities_and_counties\Long_Beach_26_06_2025\results\contact_matched.csv", index=False)
    pass

if __name__ == "__main__":
    main()