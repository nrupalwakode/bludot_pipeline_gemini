import pandas as pd
import re
from pathlib import Path
import os
from city_details import CITY_NAME

def normalize_phone(phone):
    """Extract only digits from phone number."""
    if pd.isna(phone) or str(phone).strip() == '':
        return None
    return re.sub(r'\D', '', str(phone))

def is_phone_duplicate(phone1, phone2):
    """Check if two normalized phone numbers are duplicates.
    Returns True if the shorter number appears at the end of the longer one."""
    if not phone1 or not phone2:
        return False
    if phone1 == phone2:
        return True
    
    longer = phone1 if len(phone1) >= len(phone2) else phone2
    shorter = phone2 if len(phone1) >= len(phone2) else phone1
    
    return longer.endswith(shorter)

def normalize_column_name(col_name):
    """Normalize column name for comparison (lowercase, no spaces)."""
    return re.sub(r'\s+', '', str(col_name).lower())

def extract_field_and_number(col_name):
    """Extract base field name and number suffix from column name.
    Returns (original_base_name, number) or (None, None) if no match."""
    match = re.match(r'^(.+?)_(\d+)$', col_name)
    if match:
        return match.group(1), int(match.group(2))
    return None, None

def find_column_groups(df, field_variations):
    """Find all numbered columns for each field type.
    Returns dict: {field_type: [(col_name, number), ...]}"""
    
    groups = {field: [] for field in field_variations.keys()}
    
    for col in df.columns:
        base_name, number = extract_field_and_number(col)
        if base_name and number:
            normalized_base = normalize_column_name(base_name)
            
            # Check which field type this belongs to
            for field_type, variations in field_variations.items():
                if normalized_base in variations:
                    groups[field_type].append((col, number))
                    break
    
    # Sort by number for each group
    for field_type in groups:
        groups[field_type].sort(key=lambda x: x[1])
    
    return groups

def merge_columns(input_file, output_file=None):
    """Merge numbered columns according to specified rules."""
    
    # Define field variations (normalized)
    field_variations = {
        'business_name': [normalize_column_name('Business Name')],
        'address1': [normalize_column_name('Address1')],
        'address2': [normalize_column_name('Address2'), normalize_column_name('Address 2'), 
                     normalize_column_name('addr2'), normalize_column_name('address 2')],
        'city': [normalize_column_name('City')],
        'state': [normalize_column_name('State')],
        'zipcode': [normalize_column_name('Zipcode'), normalize_column_name('Zip Code'),normalize_column_name('Zip'), 
                    normalize_column_name('zip'),normalize_column_name('zipcode'), normalize_column_name('Postal Code')],
        'website': [normalize_column_name('Website')],
        'phonenumber': [normalize_column_name('Phonenumber'), normalize_column_name('Phone number'),
                        normalize_column_name('Phone Number'),normalize_column_name('Business Phone Number'),
                        normalize_column_name('Business Phone')]
    }
    
    # Read Excel file
    print(f"Reading file: {input_file}")
    df = pd.read_excel(input_file)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    
    # Find column groups
    column_groups = find_column_groups(df, field_variations)
    
    # Process each row
    for idx in df.index:
        # 1. Business Name - longest non-empty value
        business_cols = column_groups['business_name']
        if business_cols:
            longest_value = ''
            for col_name, _ in business_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    if len(str(value)) > len(longest_value):
                        longest_value = str(value)
            
            # Update _1 column with longest value
            target_col = business_cols[0][0]  # First column name (should be _1)
            df.at[idx, target_col] = longest_value if longest_value else ''
        
        # 2. Address1 - first non-empty
        address1_cols = column_groups['address1']
        if address1_cols:
            first_value = ''
            for col_name, _ in address1_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = address1_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 3. Address2 - first non-empty
        address2_cols = column_groups['address2']
        if address2_cols:
            first_value = ''
            for col_name, _ in address2_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = address2_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 4. City - first non-empty
        city_cols = column_groups['city']
        if city_cols:
            first_value = ''
            for col_name, _ in city_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = city_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 5. State - first non-empty
        state_cols = column_groups['state']
        if state_cols:
            first_value = ''
            for col_name, _ in state_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = state_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 6. Zipcode - first non-empty
        zipcode_cols = column_groups['zipcode']
        if zipcode_cols:
            first_value = ''
            for col_name, _ in zipcode_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = zipcode_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 7. Website - first non-empty
        website_cols = column_groups['website']
        if website_cols:
            first_value = ''
            for col_name, _ in website_cols:
                value = df.at[idx, col_name]
                if pd.notna(value) and str(value).strip() != '':
                    first_value = str(value)
                    break
            target_col = website_cols[0][0]
            df.at[idx, target_col] = first_value
        
        # 8. Phonenumber - unique normalized numbers
        phone_cols = column_groups['phonenumber']
        if phone_cols:
            unique_phones = []
            normalized_phones = []
            
            for col_name, _ in phone_cols:
                value = df.at[idx, col_name]
                normalized = normalize_phone(value)
                
                if normalized:
                    # Check if this is a duplicate
                    is_duplicate = False
                    for existing_norm in normalized_phones:
                        if is_phone_duplicate(normalized, existing_norm):
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        unique_phones.append(str(value).strip())
                        normalized_phones.append(normalized)
            
            # Combine unique phones with comma-space
            target_col = phone_cols[0][0]
            df.at[idx, target_col] = ', '.join(unique_phones) if unique_phones else ''
    
    # Save output
    if output_file is None:
        input_path = Path(input_file)
        output_file = input_path.parent / f"{input_path.stem}_merged{input_path.suffix}"
    
    print(f"Saving merged data to: {output_file}")
    df.to_excel(output_file, index=False)
    print("Done!")
    
    return df

# Example usage
if __name__ == "__main__":
    # Replace with your file path
    # Working city folder path
    new_city_path = os.path.join(os.getcwd(),'cities_and_counties', CITY_NAME)
    # Deduplication folder path
    city_data_path=os.path.join(new_city_path, 'results', 'city_data')
    input_file = os.path.join(city_data_path, 'de_duplication.xlsx')
    
    # Optional: specify output file, otherwise it will create filename_merged.xlsx
    # output_file = "your_output_file.xlsx"
    
    merge_columns(input_file)
    # merge_columns(input_file, output_file)  # with custom output name