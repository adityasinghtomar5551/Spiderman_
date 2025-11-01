import streamlit as st
import pandas as pd
import requests
import json
import time
import re
import io # Used for in-memory file handling
import openpyxl # Required by pandas for .xlsx operations

# --- Page Configuration ---
st.set_page_config(
    page_title="Taxonomic Name Matcher",
    page_icon="🧬",
    layout="wide"
)

# --- Constants ---
OTT_API_ENDPOINT = 'https://api.opentreeoflife.org/v3/tnrs/match_names'

# --- Helper Functions (from original script) ---
# These functions are "pure" and don't need UI-specific changes
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

# --- Modified Helper Functions (for Streamlit Feedback) ---

def query_ott_tnrs(names_list, description="", status_container=None):
    """Queries OTT TNRS API for a list of names, reporting to Streamlit."""
    if not names_list:
        return None
    payload = {
        'names': names_list,
        'do_approximate_matching': True,
        'verbose': False
    }
    
    # Report status to Streamlit container if provided
    if status_container:
        status_container.write(f"Querying OTT for {len(names_list)} names ({description})...")
    else:
        print(f"Querying OTT for {len(names_list)} names ({description})...") # Fallback
    
    try:
        response = requests.post(OTT_API_ENDPOINT, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if status_container:
            status_container.warning(f"API request failed ({description}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                status_container.warning(f"Response content: {e.response.text}")
        else:
            print(f"API request failed ({description}): {e}") # Fallback
        return None
    except json.JSONDecodeError:
        if status_container:
            status_container.error(f"Failed to decode JSON response ({description}): {response.text}")
        else:
            print(f"Failed to decode JSON response ({description}): {response.text}") # Fallback
        return None

def process_tnrs_results(api_response, results_dict, match_level, query_map=None, status_container=None):
    """Updates the results dictionary with matches from an API response."""
    if not api_response or 'results' not in api_response:
        if status_container:
            status_container.warning(f"Warning: Invalid API response for {match_level}")
        else:
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

# --- Main Processing Pipeline ---

def run_matching_pipeline(df, column_name, batch_size):
    """
    Runs the full taxonomy matching pipeline on the DataFrame.
    Reports progress using Streamlit elements.
    Returns: (df_merged, summary_data)
    """
    
    all_results = {}
    
    with st.status("Starting taxonomic matching...") as status:
        
        # --- Load Data & Unique Names ---
        if column_name not in df.columns:
            status.error(f"Column '{column_name}' not found. Please select a valid column.")
            return None, None
            
        try:
            df[column_name] = df[column_name].astype(str)
            unique_names = df[column_name].replace('nan', '').dropna().unique().tolist()
            unique_names = [name for name in unique_names if name and name.lower() != 'nan']
            if not unique_names:
                status.error(f"No valid names found in column '{column_name}'.")
                return None, None
            status.write(f"Found {len(unique_names)} unique non-empty scientific names.")
        except Exception as e:
            status.error(f"Error processing column '{column_name}': {e}")
            return None, None

        # --- Step 1: Initial Batch Query with Original Names ---
        status.write("--- Step 1: Querying Original Scientific Names ---")
        progress_bar = st.progress(0.0, text="Step 1 Progress")
        
        for i in range(0, len(unique_names), batch_size):
            batch = unique_names[i:i+batch_size]
            batch_result_data = query_ott_tnrs(batch, f"Original Batch {i//batch_size + 1}", status_container=status)
            process_tnrs_results(batch_result_data, all_results, 'Species - Original', status_container=status)
            
            progress_bar.progress(
                (i + len(batch)) / len(unique_names), 
                text=f"Step 1 Progress: {i + len(batch)} / {len(unique_names)}"
            )
            time.sleep(1) # Pause between batches as in original script
        
        progress_bar.empty()
        failed_names = [name for name in unique_names if name not in all_results or all_results[name].get('OTT ID') is None]
        status.write(f"--- Step 1 Complete: {len(unique_names) - len(failed_names)} initial matches, {len(failed_names)} remaining.")

        # --- Step 2: Query Cleaned Names for Failures ---
        if failed_names:
            status.write("--- Step 2: Querying Cleaned Scientific Names for Failures ---")
            cleaned_names_map = {}
            names_to_query_cleaned = []
            for name in failed_names:
                cleaned = clean_scientific_name(name)
                if cleaned and cleaned != name:
                    cleaned_names_map[cleaned] = name
                    names_to_query_cleaned.append(cleaned)
            
            if names_to_query_cleaned:
                cleaned_result_data = query_ott_tnrs(list(set(names_to_query_cleaned)), "Cleaned Names", status_container=status)
                process_tnrs_results(cleaned_result_data, all_results, 'Species - Cleaned', query_map=cleaned_names_map, status_container=status)
                time.sleep(1)
            else:
                status.write("No names needed cleaning or cleaning didn't change them.")
            
            failed_names = [name for name in failed_names if all_results[name].get('OTT ID') is None]
            status.write(f"--- Step 2 Complete: {len(failed_names)} remaining.")

        # --- Step 3: Query Genus for Remaining Failures ---
        if failed_names:
            status.write("--- Step 3: Querying Genus for Remaining Failures ---")
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
                genus_result_data = query_ott_tnrs(genera_to_query, "Genera", status_container=status)
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
                                         all_results[target_name]['Match Level'] = 'No Match Final - Genus Failed'
                time.sleep(1)
            else:
                status.write("No valid genera extracted from remaining failures.")

            failed_names = [name for name in failed_names if all_results[name].get('OTT ID') is None]
            for name in failed_names:
                 if all_results[name]['Match Level'] not in ['Genus', 'No Match Final - Genus Failed']:
                     all_results[name]['Match Level'] = 'No Match Final'
            status.write(f"--- Step 3 Complete: {len(failed_names)} definitely unmatched.")

        # --- Create Results DataFrame and Merge ---
        status.write("Finalizing and merging results...")
        if not all_results:
             status.warning("No results obtained from API.")
             return None, None
        
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

        df_merged = pd.merge(
            df,
            results_df,
            left_on=column_name,
            right_on='Scientific Name_original_lookup',
            how='left'
        )
        df_merged.drop(columns=['Scientific Name_original_lookup'], inplace=True)
        
        summary = df_merged['Match Level'].value_counts(dropna=False)
        
        status.update(label="Processing Complete!", state="complete")
    
    return df_merged, summary


# --- Streamlit UI Layout ---

st.title("🧬 Taxonomic Name Resolution App")
st.markdown("This tool matches scientific names from your Excel file against the [Open Tree of Life (OTT)](https://tree.opentreeoflife.org) database. It adds taxonomic details like OTT ID, Rank, and Matched Name back to your file.")

# --- Sidebar for Configuration ---
with st.sidebar:
    st.header("⚙️ Configuration")
    
    uploaded_file = st.file_uploader("Upload your Excel file", type="xlsx")
    
    st.markdown("---")
    
    # These options will appear after file upload
    if uploaded_file:
        header_row = st.number_input("Header Row", min_value=0, value=0, help="The row number (0-indexed) containing column headers.")
        skip_rows_input = st.text_input("Rows to Skip (optional)", "0", help="Comma-separated list of 1-indexed rows to skip (e.g., the 'units' row). '1,2,5'.")
        
        # Load data into session state
        try:
            skip_rows_list = [int(x.strip()) - 1 for x in skip_rows_input.split(',') if x.strip().isdigit()] # Convert to 0-indexed
            
            # Use @st.cache_data to avoid reloading on every interaction
            @st.cache_data
            def load_data(file, header, skiprows):
                try:
                    df = pd.read_excel(file, header=header, skiprows=skiprows, sheet_name=0)
                    df.columns = df.columns.str.strip()
                    return df
                except Exception as e:
                    st.error(f"Error reading Excel file: {e}")
                    return None
            
            df = load_data(uploaded_file, header_row, skip_rows_list)
            
            if df is not None:
                st.session_state.df = df
                st.success(f"Loaded {df.shape[0]} rows.")
            
        except Exception as e:
            st.error(f"Error parsing 'Rows to Skip': {e}")
    
    st.markdown("---")
    batch_size = st.number_input("Batch Size", min_value=50, max_value=500, value=200, help="Number of names to send in each API request.")
    output_file_name = st.text_input("Output File Name", "taxonomy_results.xlsx")


# --- Main Page Content ---

if 'df' in st.session_state:
    df = st.session_state.df
    
    # --- 1. Column Selection & Data Preview ---
    st.header("1. Select Scientific Name Column")
    
    # Try to find 'Scientific Name' by default
    default_index = 0
    if "Scientific Name" in df.columns:
        default_index = df.columns.tolist().index("Scientific Name")
        
    column_name = st.selectbox(
        "Select the column containing scientific names:",
        df.columns,
        index=default_index
    )
    
    with st.expander("Show Data Preview (first 10 rows)"):
        st.dataframe(df.head(10))

    # --- 2. Run Processing ---
    st.header("2. Run Processing")
    
    if st.button("Start Taxonomic Matching 🚀", type="primary"):
        if column_name:
            df_merged, summary = run_matching_pipeline(df, column_name, batch_size)
            
            if df_merged is not None:
                st.session_state.df_processed = df_merged
                st.session_state.match_summary = summary
                st.success("Processing complete! See results below.")
            else:
                st.error("Processing failed. Check the status messages for details.")
        else:
            st.error("Please select a column with scientific names.")

    # --- 3. Results Display ---
    st.header("3. Results")
    
    if 'df_processed' in st.session_state:
        # Show summary chart
        st.subheader("Match Level Summary")
        st.bar_chart(st.session_state.match_summary)
        
        # Show data preview
        st.subheader("Processed Data Preview")
        st.dataframe(st.session_state.df_processed.head(20))
        
        # Download button
        st.markdown("---")
        
        # Convert DataFrame to Excel in-memory
        @st.cache_data
        def convert_df_to_excel(df):
            output_buffer = io.BytesIO()
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Processed Data')
            return output_buffer.getvalue()

        excel_data = convert_df_to_excel(st.session_state.df_processed)
        
        st.download_button(
            label="📥 Download Processed Excel File",
            data=excel_data,
            file_name=output_file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Results will appear here after processing.")

else:
    st.info("Please upload an Excel file using the sidebar to begin.")