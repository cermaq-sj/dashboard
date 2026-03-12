import pandas as pd
import io

import pandas as pd
import io

def clean_numeric_columns(df):
    """
    Cleans columns that should be numeric but might contain mixed types.
    """
    # Heuristic: convert columns to numeric if they are object type 
    # and contain mostly numbers or standard missing text like "N/A", "-"
    for col in df.columns:
        if df[col].dtype == 'object':
            # Try to convert to numeric, coercing errors to NaN
            # This handles "N/A", "-", and other non-numeric strings by turning them into NaN
            # We check if it actually converted some values to avoid converting purely text columns
            
            # Skip columns that are clearly text (like names, categories)
            # Heuristic: If it has more unique string values that are not numeric-like than numeric ones, skip.
            # Simpler approach for now: if column name suggests numeric (count, weight, quantity) or if user specified.
            # Without schema, we'll try to convert. If the result is all NaN, we revert (likely a text column).
            
            temp_col = pd.to_numeric(df[col], errors='coerce')
            
            # If we successfully converted a significant portion, or if the column was meant to be numeric
            # valid_count = temp_col.notna().sum()
            # total_count = len(df)
            
            # Use specific logic for known numeric markers if needed.
            # For now, let's aggressively clean only if it looks like data.
            # A safer bet for general cleaning without schema is to leave objects unless we are sure.
            pass
            
            # Let's target specific common "bad" values explicitly first
            df[col] = df[col].replace(['N/A', 'n/a', '-', '.', ''], pd.NA)
            
    return df

def basic_cleaning(df):
    """
    Applies basic cleaning steps like ffill for merged cells on categorical columns.
    """
    # 1. Remove completely empty rows
    df.dropna(how='all', inplace=True)
    
    # 2. Handle Merged Cells (Forward Fill)
    # Strategy: Columns that are likely categorical/dimensions (Context) usually need ffill.
    # Columns that are metrics (Values) should NOT be ffilled (empty means no measurement).
    # Heuristic: 
    # - Date columns: ffill
    # - String columns with low cardinality (categories like "Center", "Unit"): ffill
    # - Numeric columns: Do NOT ffill.
    
    for col in df.columns:
        # Detect Date Columns and ffill
        if 'fecha' in str(col).lower() or 'date' in str(col).lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].ffill()
            
        # Detect Categorical Columns for ffill
        # If column is object type and has significant missing values, it might be a merged category.
        elif df[col].dtype == 'object':
            # If it's a "name" or "ID" type column, ffill is usually safe for merged reports
            if any(key in str(col).lower() for key in ['lote', 'batch', 'unidad', 'unit', 'depto', 'dep', 'center', 'centro']):
                df[col] = df[col].ffill()

    # 3. Handle Mixed Types in Numeric Columns
    # We force conversion for columns that represent data metrics
    # If we knew the schema, we'd list them. For now, we iterate constraints or do safe conversion.
    # Let's iterate and if a column is object but looks like it contains numbers mixed with text, clean it.
    
    for col in df.columns:
        # If not already date or text-category we just handled
        if df[col].dtype == 'object':
             # Try to find columns that are mostly numbers represented as strings
             # simple check: try converting to numeric.
             numeric_series = pd.to_numeric(df[col], errors='coerce')
             # If we have some valid numbers and the rest resulted in NaNs (from "N/A", "-"), use the numeric version
             if numeric_series.notna().sum() > 0:
                 # Check if the original column had values that we turned to NaN (excluding existing NaNs)
                 # If we converted "N/A" to NaN, that's good.
                 # If we converted "Group A" to NaN, that's bad (it was a label).
                 
                 # Basic heuristic: if the column name implies metric (Amount, Weight, Count, num, %)
                 if any(keyword in str(col).lower() for keyword in ['cant', 'peso', 'weight', 'num', 'total', 'avg', 'prom', '%']):
                     df[col] = numeric_series

    return df

def load_and_clean_data(uploaded_files):
    """
    Loads one or more uploaded Excel files, cleans them, and combines them into a single DataFrame.
    """
    all_data = []

    for file in uploaded_files:
        try:
            # Read Excel file
            # We try to let pandas auto-detect the engine first.
            # However, for .xls files (older format), 'xlrd' is needed.
            # For .xlsx, 'openpyxl' is default.
            # Sometimes 'File is not a zip file' means it's an .xls file masquerading as .xlsx or vice versa,
            # or it's an XML/HTML file.
            
            try:
                # Try default (usually openpyxl for xlsx, xlrd for xls if installed)
                # Read ALL sheets (sheet_name=None returns a dict of dfs)
                sheets_dict = pd.read_excel(file, sheet_name=None)
            except Exception as e:
                # Fallbacks for format issues
                file.seek(0)
                if "zip" in str(e).lower() or "format" in str(e).lower():
                    try:
                        sheets_dict = pd.read_excel(file, engine='xlrd', sheet_name=None)
                    except Exception as e2:
                        file.seek(0)
                        try:
                            dfs_list = pd.read_html(file)
                            sheets_dict = {f"Sheet{i}": d for i, d in enumerate(dfs_list)}
                        except:
                            raise e 
                else:
                    raise e
            
            # Process each sheet
            for sheet_name, df in sheets_dict.items():
                if df.empty: continue
                
                # Apply cleaning
                df = basic_cleaning(df)
                
                # Add metadata
                df['source_file'] = file.name
                df['sheet_name'] = sheet_name
                
                all_data.append(df)
            
        except Exception as e:
            # In a real app we might log this or show a specific warning
            print(f"Error processing file {file.name}: {e}")
            raise ValueError(f"No se pudo leer el archivo {file.name}. Asegúrate de que sea un Excel válido (.xlsx o .xls). Error técnico: {str(e)}")

    if not all_data:
        return pd.DataFrame()

    # Combine all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    
    return combined_df
