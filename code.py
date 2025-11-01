import pandas as pd
import requests
import json
import time
import re
import openpyxl

# --- Configuration ---
INPUT_XLSX = 'Sri Lanka Food Composition Table_20240514 (1).xlsx' # Input Excel file name
OUTPUT_XLSX = 'Sri Lanka Food Composition Table_20240514_with_taxonomy.xlsx' # Output Excel file name
OTT_API_ENDPOINT = 'https://api.opentreeoflife.org/v3/tnrs/match_names' # Default API for reading OTTs
BATCH_SIZE = 200 # Send names in batches for the initial query

# --- Helper Functions ---
# (Helper functions: clean_scientific_name, extract_genus, query_ott_tnrs, process_tnrs_results)
def clean_scientific_name(name):
    """Removes common authorities and trailing characters."""
    if not isinstance(name, str):
        return None
    cleaned = re.sub(r'\s+([A-Z][a-z]*\.?|Moench|L\.)$', '', name.strip())
    return cleaned.strip()

def extract_genus(name):
    """Extracts the first word, assumed to be the genus."""
    if not isinstance(name, str) or ' ' not in name:
        return None
    return name.split(' ', 1)[0]

def query_ott_tnrs(names_list, description=""):
    """Queries OTT TNRS API for a list of names."""
    if not names_list:
        return None
    payload = {
        'names': names_list,
        'do_approximate_matching': True,
        'verbose': False
    }
    print(f"Querying OTT for {len(names_list)} names ({description})...")
    try:
        response = requests.post(OTT_API_ENDPOINT, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed ({description}): {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response content: {e.response.text}")
        return None
    except json.JSONDecodeError:
        print(f"Failed to decode JSON response ({description}): {response.text}")
        return None

def process_tnrs_results(api_response, results_dict, match_level, query_map=None):
    """Updates the results dictionary with matches from an API response."""
    if not api_response or 'results' not in api_response:
        print(f"Warning: Invalid API response for {match_level}")
        return

    for item in api_response['results']:
        original_query_name = item['name']
        target_name = query_map.get(original_query_name, original_query_name) if query_map else original_query_name

        if target_name not in results_dict or results_dict[target_name].get('OTT ID') is None:
            if item['matches']:
                match = item['matches'][0] # Take the first match
                taxon = match['taxon']
                results_dict[target_name] = {
                    'Primary Matched Name': taxon.get('unique_name', None),
                    'Synonyms': "; ".join(taxon.get('synonyms', [])),
                    'OTT ID': taxon.get('ott_id', None),
                    'Rank': taxon.get('rank', None),
                    'Match Query': original_query_name,
                    'Match Level': match_level,
                    'Approximate Match': match.get('is_approximate_match', False),
                    'Is Synonym Input': match.get('is_synonym', False)
                }
            elif match_level == 'Species - Original' and target_name not in results_dict:
                 results_dict[target_name] = {
                    'Primary Matched Name': None, 'Synonyms': None, 'OTT ID': None,
                    'Rank': None, 'Match Query': original_query_name,
                    'Match Level': 'No Match Initial', 'Approximate Match': False, 'Is Synonym Input': False
                 }

# --- Load Data ---
try:
    # Assumes data is in the first sheet (sheet_name=0)
    # Assumes header is row 0 (header=0)
    # Assumes the units row (row 1) needs skipping (skiprows=[1])
    df = pd.read_excel(INPUT_XLSX, header=0, skiprows=[1], sheet_name=0)
    # <<< END CHANGED >>>

    print(f"Loaded DataFrame from '{INPUT_XLSX}' with shape: {df.shape}")
    if 'Scientific Name' not in df.columns:
        # Add check for leading/trailing spaces in column names
        df.columns = df.columns.str.strip()
        if 'Scientific Name' not in df.columns:
             raise ValueError("Column 'Scientific Name' not found.")

    # Handle potential NaN values explicitly if necessary and convert to string
    df['Scientific Name'] = df['Scientific Name'].astype(str)
    unique_names = df['Scientific Name'].replace('nan', '').dropna().unique().tolist()
    unique_names = [name for name in unique_names if name and name.lower() != 'nan'] # Remove empty strings and 'nan' strings
    print(f"Found {len(unique_names)} unique non-empty scientific names.")

except FileNotFoundError:
    print(f"Error: Input file '{INPUT_XLSX}' not found.")
    unique_names = []
    df = pd.DataFrame()
except Exception as e:
    print(f"Error loading or processing Excel file: {e}")
    unique_names = []
    df = pd.DataFrame()


# --- Main Processing Logic ---
all_results = {}

if unique_names:
    # --- Step 1: Initial Batch Query with Original Names ---
    print("\n--- Step 1: Querying Original Scientific Names ---")
    for i in range(0, len(unique_names), BATCH_SIZE):
        batch = unique_names[i:i+BATCH_SIZE]
        batch_result_data = query_ott_tnrs(batch, description=f"Original Batch {i//BATCH_SIZE + 1}")
        process_tnrs_results(batch_result_data, all_results, 'Species - Original')
        time.sleep(1) # Pause between batches

    failed_names = [name for name in unique_names if name not in all_results or all_results[name].get('OTT ID') is None]
    print(f"\n--- Step 1 Complete: {len(unique_names) - len(failed_names)} initial matches, {len(failed_names)} remaining.")

    # --- Step 2: Query Cleaned Names for Failures ---
    if failed_names:
        print("\n--- Step 2: Querying Cleaned Scientific Names for Failures ---")
        cleaned_names_map = {}
        names_to_query_cleaned = []
        for name in failed_names:
            cleaned = clean_scientific_name(name)
            if cleaned and cleaned != name:
                cleaned_names_map[cleaned] = name
                names_to_query_cleaned.append(cleaned)
        if names_to_query_cleaned:
            cleaned_result_data = query_ott_tnrs(list(set(names_to_query_cleaned)), description="Cleaned Names") # Use set to avoid duplicate queries
            process_tnrs_results(cleaned_result_data, all_results, 'Species - Cleaned', query_map=cleaned_names_map)
            time.sleep(1)
        else:
            print("No names needed cleaning or cleaning didn't change them.")
        failed_names = [name for name in failed_names if all_results[name].get('OTT ID') is None]
        print(f"--- Step 2 Complete: {len(failed_names)} remaining.")

    # --- Step 3: Query Genus for Remaining Failures ---
    if failed_names:
        print("\n--- Step 3: Querying Genus for Remaining Failures ---")
        genus_map = {}
        genera_to_query = []
        for name in failed_names:
            genus = extract_genus(name)
            if genus:
                if genus not in genera_to_query:
                     genera_to_query.append(genus)
                if genus not in genus_map:
                    genus_map[genus] = []
                genus_map[genus].append(name)
        if genera_to_query:
            genus_result_data = query_ott_tnrs(genera_to_query, description="Genera")
            if genus_result_data and 'results' in genus_result_data:
                 for item in genus_result_data['results']:
                    genus_query_name = item['name']
                    if genus_query_name in genus_map:
                        original_names_for_genus = genus_map[genus_query_name]
                        for target_name in original_names_for_genus:
                            if all_results[target_name].get('OTT ID') is None:
                                if item['matches']:
                                    match = item['matches'][0]
                                    taxon = match['taxon']
                                    if taxon.get('rank', '').lower() in ['genus', 'family', 'order', 'class', 'phylum', 'kingdom']:
                                         all_results[target_name] = {
                                            'Primary Matched Name': taxon.get('unique_name', None), 'Synonyms': "; ".join(taxon.get('synonyms', [])),
                                            'OTT ID': taxon.get('ott_id', None), 'Rank': taxon.get('rank', None),
                                            'Match Query': genus_query_name, 'Match Level': 'Genus',
                                            'Approximate Match': match.get('is_approximate_match', False), 'Is Synonym Input': match.get('is_synonym', False)
                                        }
                                else:
                                     all_results[target_name]['Match Level'] = 'No Match Final - Genus Failed' # More specific failure
            time.sleep(1)
        else:
            print("No valid genera extracted from remaining failures.")

        failed_names = [name for name in failed_names if all_results[name].get('OTT ID') is None]
        for name in failed_names:
             if all_results[name]['Match Level'] not in ['Genus', 'No Match Final - Genus Failed']:
                 all_results[name]['Match Level'] = 'No Match Final'
        print(f"--- Step 3 Complete: {len(failed_names)} definitely unmatched.")

# --- Create Results DataFrame and Merge ---
if not all_results:
     print("No results obtained from API.")
elif df.empty:
    print("Original DataFrame is empty, cannot merge.")
else:
    for name in unique_names:
        if name not in all_results:
             all_results[name] = {
                'Primary Matched Name': None, 'Synonyms': None, 'OTT ID': None, 'Rank': None,
                'Match Query': name, 'Match Level': 'Processing Error', 'Approximate Match': False, 'Is Synonym Input': False
             }
        elif all_results[name].get('OTT ID') is None and all_results[name]['Match Level'] == 'No Match Initial':
             all_results[name]['Match Level'] = 'No Match Final'

    results_df = pd.DataFrame.from_dict(all_results, orient='index')
    results_df.index.name = 'Scientific Name_original_lookup'
    results_df.reset_index(inplace=True)

    if 'Scientific Name_original_lookup' not in results_df.columns:
         print("Error: Lookup column missing in results DataFrame.")
    elif 'Scientific Name' not in df.columns:
         print("Error: 'Scientific Name' column missing in original DataFrame for merge.")
    else:
        print("\nMerging results back into DataFrame...")
        df_merged = pd.merge(
            df,
            results_df,
            left_on='Scientific Name',
            right_on='Scientific Name_original_lookup',
            how='left'
        )
        df_merged.drop(columns=['Scientific Name_original_lookup'], inplace=True)

        # --- Save Output ---
        try:
            # index=False prevents writing the DataFrame index as a column
            # sheet_name specifies the name of the sheet in the output file
            df_merged.to_excel(OUTPUT_XLSX, index=False, sheet_name='Processed Data')
            # <<< END CHANGED >>>

            print(f"\nSuccessfully saved augmented data to {OUTPUT_XLSX}")
            print(f"Final DataFrame shape: {df_merged.shape}")
            if 'Match Level' in df_merged.columns:
                print("\nMatch Level Summary:")
                print(df_merged['Match Level'].value_counts(dropna=False))
            else:
                 print("Match Level column not found in merged df.")

        except Exception as e:
            # Provide more specific error for permission issues
            if isinstance(e, PermissionError):
                 print(f"Error saving output Excel file: Permission denied. Is '{OUTPUT_XLSX}' open or write-protected?")
            else:
                 print(f"Error saving output Excel file: {e}")