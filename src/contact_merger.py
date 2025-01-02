import pandas as pd
from deep_translator import GoogleTranslator

def is_hebrew(text):
    if not isinstance(text, str):
        return False
    hebrew_range = range(0x590, 0x5FF)
    return any(ord(char) in hebrew_range for char in text)

def translate_text(text):
    if not isinstance(text, str):
        return text
    if not is_hebrew(text):
        return text
    
    try:
        translator = GoogleTranslator(source='iw', target='en')
        return translator.translate(text)
    except Exception as e:
        print(f"Translation error for text '{text}': {str(e)}")
        return text

def process_contacts(df):
    """
    Process contacts and handle translations.
    """
    df = df.copy()
    
    # If there's no English Name column but there's a Name column
    if 'English Name' not in df.columns and 'Name' in df.columns:
        df['English Name'] = df['Name']
        df['Hebrew Name'] = df['Name']
    
    # If there's an English Name but no Hebrew Name
    if 'Hebrew Name' not in df.columns:
        df['Hebrew Name'] = df['English Name']
    
    # Process each row
    for idx, row in df.iterrows():
        try:
            name = row.get('English Name', row.get('Name', ''))
            hebrew_name = row.get('Hebrew Name', name)
            
            if pd.isna(name) or name == '':
                name = hebrew_name
            
            if is_hebrew(str(name)):
                english_name = translate_text(str(name))
                df.at[idx, 'English Name'] = english_name
                df.at[idx, 'Hebrew Name'] = name
            else:
                df.at[idx, 'English Name'] = name
                if pd.isna(df.at[idx, 'Hebrew Name']):
                    df.at[idx, 'Hebrew Name'] = name
        except Exception as e:
            print(f"Error processing row {idx}: {str(e)}")
            continue
    
    return df

def merge_duplicates(df):
    """
    Merge duplicate contacts based on email, phone, or name.
    Prints detailed merge information and returns the merged dataframe.
    """
    df = df.copy()
    
    # Initialize a list to store merge information
    merge_info = []
    
    # Function to merge two rows
    def merge_rows(row1, row2):
        merged = {}
        for col in row1.index:
            val1, val2 = row1[col], row2[col]
            # Take non-NaN value if one is NaN
            if pd.isna(val1) and not pd.isna(val2):
                merged[col] = val2
            elif pd.isna(val2) and not pd.isna(val1):
                merged[col] = val1
            # If both are non-NaN and different, keep both values separated by ' | '
            elif not pd.isna(val1) and not pd.isna(val2) and val1 != val2:
                merged[col] = f"{val1} | {val2}"
            else:
                merged[col] = val1
        return pd.Series(merged)

    # First, find duplicates based on email (excluding empty emails)
    email_dupes = df[df['Email'].notna() & (df['Email'] != '')].duplicated(subset=['Email'], keep=False)
    email_groups = df[email_dupes].groupby('Email')
    
    for email, group in email_groups:
        if len(group) > 1:
            merge_info.append(f"\nMerging contacts with email {email}:")
            for _, row in group.iterrows():
                merge_info.append(f"- {row['English Name']} ({row['Phone']})")
            
            # Merge all rows in the group
            merged_row = group.iloc[0]
            for i in range(1, len(group)):
                merged_row = merge_rows(merged_row, group.iloc[i])
            
            # Update the first occurrence and remove others
            df.loc[group.index[0]] = merged_row
            df = df.drop(group.index[1:])

    # Then find duplicates based on phone (excluding empty phones)
    phone_dupes = df[df['Phone'].notna() & (df['Phone'] != '')].duplicated(subset=['Phone'], keep=False)
    phone_groups = df[phone_dupes].groupby('Phone')
    
    for phone, group in phone_groups:
        if len(group) > 1:
            merge_info.append(f"\nMerging contacts with phone {phone}:")
            for _, row in group.iterrows():
                merge_info.append(f"- {row['English Name']} ({row['Email']})")
            
            merged_row = group.iloc[0]
            for i in range(1, len(group)):
                merged_row = merge_rows(merged_row, group.iloc[i])
            
            df.loc[group.index[0]] = merged_row
            df = df.drop(group.index[1:])

    # Finally, find duplicates based on English Name (excluding empty names)
    name_dupes = df[df['English Name'].notna() & (df['English Name'] != '')].duplicated(subset=['English Name'], keep=False)
    name_groups = df[name_dupes].groupby('English Name')
    
    for name, group in name_groups:
        if len(group) > 1:
            merge_info.append(f"\nMerging contacts with name {name}:")
            for _, row in group.iterrows():
                merge_info.append(f"- {row['Email']} ({row['Phone']})")
            
            merged_row = group.iloc[0]
            for i in range(1, len(group)):
                merged_row = merge_rows(merged_row, group.iloc[i])
            
            df.loc[group.index[0]] = merged_row
            df = df.drop(group.index[1:])

    # Print all merge information
    if merge_info:
        print("\nMerge Operations:")
        print("\n".join(merge_info))
    else:
        print("No duplicates found to merge.")

    return df.reset_index(drop=True)

def clean_and_merge_contacts(df):
    """
    Clean, translate, and merge contacts from a DataFrame.
    """
    df = df.copy()
    
    # Standardize column names
    if 'Hebrew_Name' in df.columns:
        df = df.rename(columns={'Hebrew_Name': 'Hebrew Name'})
    if 'English_Name' in df.columns:
        df = df.rename(columns={'English_Name': 'English Name'})
    
    # Ensure required columns exist
    required_columns = ['English Name', 'Hebrew Name', 'Email', 'Phone', 'Google Contact ID', 'Labels']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    # Process translations in batches to avoid memory issues
    batch_size = 100
    processed_dfs = []
    
    for start_idx in range(0, len(df), batch_size):
        end_idx = min(start_idx + batch_size, len(df))
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        try:
            # Process translations for this batch
            batch_df = process_contacts(batch_df)
            
            # Clean phone numbers (standardize format)
            batch_df['Phone'] = batch_df['Phone'].apply(lambda x: str(x).strip() if pd.notna(x) else '')
            batch_df['Phone'] = batch_df['Phone'].replace(r'\s+', '', regex=True)  # Remove spaces
            batch_df['Phone'] = batch_df['Phone'].replace(r'^0', '+972', regex=True)  # Convert leading 0 to +972
            
            # Clean emails (lowercase and strip)
            batch_df['Email'] = batch_df['Email'].apply(lambda x: str(x).lower().strip() if pd.notna(x) else '')
            
            # Add to processed batches
            processed_dfs.append(batch_df)
            
            print(f"Processed batch {start_idx//batch_size + 1} ({start_idx} to {end_idx})")
            
        except Exception as e:
            print(f"Error processing batch {start_idx//batch_size + 1}: {str(e)}")
            # Continue with next batch even if this one fails
            processed_dfs.append(df.iloc[start_idx:end_idx].copy())
    
    # Combine all processed batches
    df = pd.concat(processed_dfs, axis=0, ignore_index=True)
    
    # Merge duplicates
    df = merge_duplicates(df)
    
    # Final cleanup
    df = df.fillna('')
    
    # Ensure 'Merkaz' is in Labels where appropriate
    df['Labels'] = df['Labels'].apply(lambda x: 'Merkaz' if pd.isna(x) or str(x).strip() == '' else x)
    
    # Return only required columns
    final_columns = ['English Name', 'Hebrew Name', 'Email', 'Phone', 'Google Contact ID', 'Labels']
    return df[final_columns]

if __name__ == "__main__":
    # Test the complete workflow
    test_df = pd.DataFrame({
        'Name': ['שלום עולם', 'John Doe', 'דוד כהן'],
        'Email': ['test@test.com', 'john@example.com', 'david@test.com'],
        'Phone': ['0541234567', '972541234567', '054-123-4567']
    })
    
    print("Original DataFrame:")
    print(test_df)
    print("\nAfter cleaning and merging:")
    result = clean_and_merge_contacts(test_df)
    print(result)