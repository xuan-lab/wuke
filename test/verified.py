import pandas as pd
import pubchempy as pcp
import time
import os
import math # To check for NaN
import re   # To clean strings for float conversion
import argparse # << NEW: For command-line arguments >>
import sys # << NEW: For command-line arguments >>
import logging # << NEW: For logging >>

# --- Dependencies ---
# This script requires 'pubchempy', 'pandas', and 'openpyxl'.
# If you don't have them installed, run: pip install pubchempy pandas openpyxl

# --- Configuration ---
# --- Input/Output filenames are now handled by command-line arguments ---
# output_file_detailed = 'pubchem_verified_data.xlsx' # Removed hardcoded name
# output_file_summary = 'pubchem_verification_summary_by_name.xlsx' # Removed hardcoded name << REMOVED >>

# !!! IMPORTANT: Define column names from your input file used for hierarchical lookup !!!
# The script will try these in order. Ensure they match your Excel file exactly.
name_col = 'molecule_name'     # 1st priority for lookup (and used for summary)
alias_col = 'Alias'            # 2nd priority for lookup
cas_id_col = 'CAS_id'          # Used for cross-validation check reporting
pubchem_id_col = 'PubChem_id'  # 3rd priority for lookup

# Limit the number of subsequent aliases to try IF the first alias fails
max_subsequent_aliases_to_try = 2 # e.g., Try alias[1] and alias[2] if alias[0] fails

# Delay between PubChem API requests (in seconds)
api_delay = 0.3 # seconds

# --- Mapping from potential input column names (lowercase) to PubChemPy attributes ---
# This map is used AFTER a compound is found, for detailed comparison.
PUBCHEM_PROPERTY_MAP = {
    # Input Column Name (lowercase, normalized): PubChemPy Attribute Name or Special Handler
    'molecule_name': 'iupac_name', # Also used to identify the name column for summary
    'iupac name': 'iupac_name',
    'iupac_name': 'iupac_name',
    'alias': 'synonyms', # Special handling might be needed if comparing list vs string
    'smiles': 'canonical_smiles',
    'canonical_smiles': 'canonical_smiles',
    'isomeric_smiles': 'isomeric_smiles',
    'molecular_formula': 'molecular_formula',
    'formula': 'molecular_formula',
    'molecular_weight': 'molecular_weight',
    'mw': 'molecular_weight',
    'exact_mass': 'exact_mass',
    'monoisotopic_mass': 'monoisotopic_mass',
    'inchi': 'inchi',
    'inchikey': 'inchikey',
    'charge': 'charge',
    'h_bond_donor_count': 'h_bond_donor_count',
    'hdon': 'h_bond_donor_count', # Added mapping
    'h_bond_acceptor_count': 'h_bond_acceptor_count',
    'hacc': 'h_bond_acceptor_count', # Added mapping
    'rotatable_bond_count': 'rotatable_bond_count',
    'heavy_atom_count': 'heavy_atom_count',
    'isotope_atom_count': 'isotope_atom_count',
    'defined_atom_stereo_count': 'defined_atom_stereo_count',
    'undefined_atom_stereo_count': 'undefined_atom_stereo_count',
    'defined_bond_stereo_count': 'defined_bond_stereo_count',
    'undefined_bond_stereo_count': 'undefined_bond_stereo_count',
    'covalent_unit_count': 'covalent_unit_count',
    'xlogp': 'xlogp',
    'alogp': 'xlogp', # Added mapping (assuming alogp relates to xlogp)
    'tpsa': 'tpsa',
    # --- Special Handling for CAS ---
    'cas': 'cas_from_synonyms',
    'cas_id': 'cas_from_synonyms',
    'cas_number': 'cas_from_synonyms',
    'casid': 'cas_from_synonyms',
    # --- Columns likely NOT directly comparable with standard PubChem attributes ---
    # ... (other non-comparable columns) ...
}

# --- Regular Expression for CAS Number Validation ---
CAS_REGEX = re.compile(r'^\d{2,7}-\d{2}-\d$')

# --- Helper Functions (is_nan_or_none, clean_for_float, normalize_column_name, get_valid_cas) ---
def is_nan_or_none(value):
    if value is None: return True
    try:
        if pd.isna(value): return True
        try: return math.isnan(float(value))
        except ValueError: return False
    except (TypeError): return False

def clean_for_float(value):
    if is_nan_or_none(value): return None
    try:
        s_value = str(value)
        match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s_value)
        if match: return float(match.group(0))
        else:
            cleaned_s = re.sub(r"[^0-9.-]", "", s_value.split()[0])
            if cleaned_s and cleaned_s != '.' and cleaned_s != '-': return float(cleaned_s)
            return None
    except (ValueError, TypeError): return None

def normalize_column_name(name):
    if not isinstance(name, str): name = str(name)
    return name.lower().replace(' ', '_').replace('-', '_')

def get_valid_cas(cas_string):
    """ Checks if a string matches CAS format and returns it, otherwise None. """
    if isinstance(cas_string, str):
        match = CAS_REGEX.match(cas_string.strip())
        if match:
            return match.group(0)
    return None

def get_valid_cid(cid_value):
    """ Converts input to a valid integer CID, returns None if invalid. """
    if not is_nan_or_none(cid_value) and str(cid_value).strip() != '':
        try:
            return int(float(cid_value))
        except (ValueError, TypeError):
            return None
    return None
# --- End of Helper Functions ---

# --- Function to find the actual name column used for summary --- << REMOVED >>
# def find_name_column(df_columns):
#     target_attr = 'iupac_name'
#     possible_map_keys = [key for key, val in PUBCHEM_PROPERTY_MAP.items() if val == target_attr]
#     if not possible_map_keys: return None
#     for col in df_columns:
#         normalized_col = normalize_column_name(col)
#         if normalized_col in possible_map_keys:
#             return col
#     return None

# --- Main Verification Script ---
if __name__ == "__main__":
    # --- Argument Parsing --- <<< NEW >>>
    parser = argparse.ArgumentParser(description='PubChem 数据核对与更新脚本 (名称优先查找 + 仅报告交叉验证)')
    parser.add_argument('input_file', type=str, help='输入的包含待核对数据的 Excel 文件路径 (.xlsx)')
    # Check if any arguments were passed, provide help if not
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()
    input_file = args.input_file

    # --- Derive Output Filenames --- <<< MODIFIED >>>
    # input_dir = os.path.dirname(input_file) # No longer needed for output dir
    base_name, ext = os.path.splitext(os.path.basename(input_file))
    if ext.lower() != '.xlsx':
         print(f"错误：输入文件 '{input_file}' 必须是 .xlsx 格式。")
         sys.exit(1)

    # --- Setup Logging --- <<< NEW >>>
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Get script directory
    log_dir = os.path.join(script_dir, 'log')
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = os.path.splitext(os.path.basename(__file__))[0] + '.log'
    log_file_path = os.path.join(log_dir, log_file_name)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'), # Write mode overwrites old log
            logging.StreamHandler() # Also print logs to console
        ]
    )
    # --- End of Logging Setup ---

    # Define and create the output directory <<< MODIFIED >>>
    output_dir = os.path.join(script_dir, 'verified_file') # Output dir relative to script
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"输出目录: {output_dir}")

    # Define output file paths within the new directory
    output_file_detailed = os.path.join(output_dir, f"{base_name}_verified{ext}")
    # output_file_summary = os.path.join(output_dir, f"{base_name}_summary{ext}") # << REMOVED >>

    logging.info(f"输入文件: {input_file}")
    logging.info(f"详细输出文件: {output_file_detailed}")
    # logging.info(f"摘要输出文件: {output_file_summary}") # << REMOVED >>
    logging.info(f"日志文件: {log_file_path}") # Log the log file path

    if not os.path.exists(input_file):
        logging.error(f"错误：输入文件 '{input_file}' 未找到。")
        exit()

    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        logging.info(f"文件加载成功，包含 {len(df)} 行数据和以下列: {list(df.columns)}")
    except ImportError: logging.error("错误：缺少 'openpyxl' 库。"); exit()
    except Exception as e: logging.error(f"加载 Excel 文件时出错: {e}"); exit()

    # --- Validate essential lookup and validation columns exist ---
    lookup_cols_needed = [name_col, alias_col, pubchem_id_col]
    validation_cols_needed = [cas_id_col, pubchem_id_col]
    required_cols = list(set(lookup_cols_needed + validation_cols_needed))
    missing_required_cols = [col for col in required_cols if col not in df.columns]
    if missing_required_cols:
        logging.error(f"错误：输入文件中缺少用于查找或交叉验证报告的必要列: {', '.join(missing_required_cols)}")
        exit()
    if cas_id_col not in df.columns: # Check specifically for CAS column for warning
         logging.warning(f"警告: 输入文件中缺少列 '{cas_id_col}'，交叉验证检查将受限。")


    status_column = 'PubChem_Verification_Status'
    if status_column not in df.columns:
        df[status_column] = '未处理'

    logging.info("\n开始 PubChem 数据核对与更新 (名称优先查找 + 仅报告交叉验证)...")
    logging.info(f"查找顺序: '{name_col}' -> 第1个'{alias_col}' -> 后续'{alias_col}' (最多{max_subsequent_aliases_to_try}个, 检查内部一致性) -> '{pubchem_id_col}'")
    logging.info("交叉验证: 对名称/别名匹配结果进行检查，但仅报告不一致，不否决匹配。")
    logging.warning("警告：优先使用名称/别名查找仍可能存在风险！")
    logging.info(f"注意：每次查询 PubChem 之间将有 {api_delay} 秒的延时。")

    all_input_columns = df.columns.tolist()

    # --- Log Optimization: Counters ---
    processed_rows = 0
    consistent_rows_count = 0
    updated_indices = set()
    error_indices = set()
    multi_match_indices = set()
    no_match_indices = set()
    cross_validation_failures_reported = 0


    # --- Main Checking Loop ---
    for index, row in df.iterrows():
        processed_rows += 1
        overall_status = "未处理"
        found_compound = None
        found_by = None
        potential_compound = None
        cross_validation_passed = True

        original_cid_for_validation = get_valid_cid(row.get(pubchem_id_col))
        original_cas_for_validation = get_valid_cas(row.get(cas_id_col)) if cas_id_col in df.columns else None # Handle missing CAS col

        # --- Hierarchical Lookup (First Alias Priority, Reporting-Only Cross-Validation) ---
        # (Lookup logic remains the same as previous version)
        # Step 1: Try finding by Name
        name_value = row.get(name_col)
        if not is_nan_or_none(name_value) and str(name_value).strip() != '':
            try:
                cids = pcp.get_cids(name_value, 'name')
                if len(cids) == 1:
                    potential_compound = pcp.Compound.from_cid(cids[0])
                    time.sleep(api_delay)
                    if potential_compound:
                        validated = False
                        if original_cid_for_validation and potential_compound.cid == original_cid_for_validation: validated = True
                        elif original_cas_for_validation and potential_compound.synonyms:
                             pubchem_cas_list_for_val = [get_valid_cas(syn) for syn in potential_compound.synonyms if get_valid_cas(syn)]
                             if original_cas_for_validation in pubchem_cas_list_for_val: validated = True
                        elif not original_cid_for_validation and not original_cas_for_validation: validated = True
                        found_compound = potential_compound; found_by = name_col # Accept match regardless
                        if not validated: cross_validation_passed = False # Mark validation failed
                elif len(cids) > 1: overall_status = f"通过名称 '{name_value}' 找到多个匹配 ({len(cids)}个)"; multi_match_indices.add(index)
                else: overall_status = f"通过名称 '{name_value}' 未找到匹配"
            except Exception as e: overall_status = f"查找名称 '{name_value}' 时出错: {e}"; logging.error(f"错误: 行 {index + 2} - {overall_status}"); error_indices.add(index)
            time.sleep(api_delay)

        # Step 2: If not found by Name, try finding by Alias
        if not found_compound and alias_col in df.columns:
            alias_value = row.get(alias_col)
            potential_compound_alias = None
            consistent_cid_from_aliases = None
            found_by_alias_detail = None
            if not is_nan_or_none(alias_value) and isinstance(alias_value, str) and alias_value.strip() != '':
                aliases = [a.strip() for a in alias_value.split(';') if a.strip()]
                if aliases:
                    first_alias = aliases[0]; first_alias_found_cid = None
                    try:
                        cids = pcp.get_cids(first_alias, 'name'); time.sleep(api_delay)
                        if len(cids) == 1: first_alias_found_cid = cids[0]
                        elif len(cids) > 1:
                             if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"第一个别名 '{first_alias}' 找到多个匹配 ({len(cids)}个)"; multi_match_indices.add(index)
                    except Exception as e: logging.warning(f"警告: 行 {index + 2}, 查找第一个别名 '{first_alias}' 时出错: {e}"); error_indices.add(index)

                    if first_alias_found_cid:
                         try:
                             potential_compound = pcp.Compound.from_cid(first_alias_found_cid); time.sleep(api_delay)
                             if potential_compound:
                                 validated = False
                                 if original_cid_for_validation and potential_compound.cid == original_cid_for_validation: validated = True
                                 elif original_cas_for_validation and potential_compound.synonyms:
                                      pubchem_cas_list_for_val = [get_valid_cas(syn) for syn in potential_compound.synonyms if get_valid_cas(syn)]
                                      if original_cas_for_validation in pubchem_cas_list_for_val: validated = True
                                 elif not original_cid_for_validation and not original_cas_for_validation: validated = True
                                 found_compound = potential_compound; found_by = f"{alias_col} ('{first_alias}')"
                                 if not validated: cross_validation_passed = False
                         except Exception as e: overall_status = f"获取第一个别名匹配的 CID {first_alias_found_cid} 时出错: {e}"; logging.error(f"错误: 行 {index + 2} - {overall_status}"); error_indices.add(index);
                    elif len(aliases) > 1:
                        aliases_tried_str = ""; found_cids_from_aliases = set(); cid_to_alias_map = {}; alias_found_multi = False
                        aliases_to_attempt = aliases[1 : 1 + max_subsequent_aliases_to_try]; aliases_tried_str = ', '.join([f"'{a}'" for a in aliases_to_attempt])
                        if aliases_to_attempt:
                            for individual_alias in aliases_to_attempt:
                                try:
                                    cids = pcp.get_cids(individual_alias, 'name'); time.sleep(api_delay)
                                    if len(cids) == 1:
                                        found_cid = cids[0]; found_cids_from_aliases.add(found_cid)
                                        if found_cid not in cid_to_alias_map: cid_to_alias_map[found_cid] = individual_alias
                                    elif len(cids) > 1: alias_found_multi = True
                                except Exception as e: logging.warning(f"警告: 行 {index + 2}, 查找后续别名 '{individual_alias}' 时出错: {e}"); error_indices.add(index)
                            if len(found_cids_from_aliases) == 1:
                                consistent_cid_from_aliases = list(found_cids_from_aliases)[0]; found_by_alias_detail = cid_to_alias_map.get(consistent_cid_from_aliases, aliases_to_attempt[0])
                                try:
                                    potential_compound_alias = pcp.Compound.from_cid(consistent_cid_from_aliases); time.sleep(api_delay)
                                    if potential_compound_alias:
                                         validated = False
                                         if original_cid_for_validation and potential_compound_alias.cid == original_cid_for_validation: validated = True
                                         elif original_cas_for_validation and potential_compound_alias.synonyms:
                                              pubchem_cas_list_for_val = [get_valid_cas(syn) for syn in potential_compound_alias.synonyms if get_valid_cas(syn)]
                                              if original_cas_for_validation in pubchem_cas_list_for_val: validated = True
                                         elif not original_cid_for_validation and not original_cas_for_validation: validated = True
                                         found_compound = potential_compound_alias; found_by = f"{alias_col} ('{found_by_alias_detail}')"
                                         if not validated: cross_validation_passed = False
                                except Exception as e: overall_status = f"获取后续别名匹配的 CID {consistent_cid_from_aliases} 时出错: {e}"; logging.error(f"错误: 行 {index + 2} - {overall_status}"); error_indices.add(index);
                            elif len(found_cids_from_aliases) > 1:
                                if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"通过尝试后续别名 {aliases_tried_str} 找到多个不同匹配 CIDs: {list(found_cids_from_aliases)}"; multi_match_indices.add(index)
                            else:
                                 current_status_prefix = overall_status if overall_status != "未处理" else ""
                                 subsequent_alias_status = ""
                                 if alias_found_multi: subsequent_alias_status = f"后续别名 {aliases_tried_str} 时找到多个匹配" ; multi_match_indices.add(index)
                                 else: subsequent_alias_status = f"后续别名 {aliases_tried_str} 未找到唯一匹配"
                                 overall_status = f"{current_status_prefix}; {subsequent_alias_status}".strip("; ")
                    elif len(aliases) <= 1:
                         if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"第一个别名 '{aliases[0]}' 未找到唯一匹配"
                elif not aliases:
                     if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"别名列 '{alias_col}' 内容无效或为空"

        # Step 3: If not found by Name or Alias, try finding by PubChem ID
        if not found_compound:
            cid_value_lookup = row.get(pubchem_id_col); cid_to_lookup = get_valid_cid(cid_value_lookup)
            if cid_to_lookup:
                try:
                    compound = pcp.Compound.from_cid(cid_to_lookup);
                    if compound: found_compound = compound; found_by = pubchem_id_col
                except pcp.NotFoundError:
                    if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"通过 PubChem ID {cid_to_lookup} 未找到"
                except Exception as e: overall_status = f"查找 PubChem ID {cid_to_lookup} 时出错: {e}"; logging.error(f"错误: 行 {index + 2} - {overall_status}"); error_indices.add(index)
                time.sleep(api_delay)
            elif not is_nan_or_none(cid_value_lookup):
                 if overall_status == "未处理" or "未找到匹配" in overall_status: overall_status = f"PubChem ID '{cid_value_lookup}' 格式无效"; error_indices.add(index)

        # --- Step 4: Perform Detailed Comparison if a unique compound was found ---
        if found_compound:
            # (Comparison logic remains the same, including conditional PubChem ID update and name/CAS protection)
            # ... (omitted for brevity, same as previous version) ...
            updates_made_cols = []
            current_row_status_parts = []
            row_updated_flag = False

            # Add cross-validation failure note if applicable
            if not cross_validation_passed and found_by and (found_by == name_col or found_by.startswith(alias_col)):
                 current_row_status_parts.append("(交叉验证失败:与原始ID/CAS不符)")
                 cross_validation_failures_reported += 1

            # Check and Update PubChem ID if found by Name/Alias (Reverted Logic)
            if found_by and (found_by == name_col or found_by.startswith(alias_col)):
                found_cid = found_compound.cid
                original_cid_value = row.get(pubchem_id_col)
                original_cid = None; original_id_is_valid = False
                if not is_nan_or_none(original_cid_value) and str(original_cid_value).strip() != '':
                    try: original_cid = int(float(original_cid_value)); original_id_is_valid = True
                    except (ValueError, TypeError): original_id_is_valid = False
                if not original_id_is_valid or (original_id_is_valid and original_cid != found_cid):
                    df.loc[index, pubchem_id_col] = found_cid
                    identifier_detail = found_by.split('(')[-1].split(')')[0].strip("'") if '(' in str(found_by) else found_by
                    current_row_status_parts.append(f"{pubchem_id_col}(根据'{identifier_detail}'匹配更新为{found_cid})")
                    updates_made_cols.append(pubchem_id_col); updated_indices.add(index); row_updated_flag = True
                elif original_cid == found_cid: pass

            # --- Comparison Loop ---
            columns_to_compare = [col for col in all_input_columns if col not in [pubchem_id_col, status_column]]
            for input_col_name in columns_to_compare:
                normalized_col = normalize_column_name(input_col_name)
                col_status = ""; update_needed = False; value_to_update = None
                if normalized_col in PUBCHEM_PROPERTY_MAP:
                    pubchem_attr_name = PUBCHEM_PROPERTY_MAP[normalized_col]
                    if pubchem_attr_name is None: continue
                    original_value = row[input_col_name]
                    # CAS Handling
                    if pubchem_attr_name == 'cas_from_synonyms':
                        pubchem_cas_list = []; original_cas = get_valid_cas(original_value)
                        if found_compound.synonyms:
                            for syn in found_compound.synonyms:
                                valid_cas = get_valid_cas(syn);
                                if valid_cas: pubchem_cas_list.append(valid_cas)
                        if original_cas:
                            if pubchem_cas_list:
                                if original_cas in pubchem_cas_list: col_status = f"{input_col_name}(CAS 一致)"
                                else: col_status = f"{input_col_name}(CAS 不匹配 PubChem: {pubchem_cas_list})"
                            else: col_status = f"{input_col_name}(原始 CAS 有效，PubChem 未找到)"
                        else:
                            if len(pubchem_cas_list) == 1: col_status = f"{input_col_name}(用 PubChem 唯一 CAS 更新)"; update_needed = True; value_to_update = pubchem_cas_list[0]
                            elif len(pubchem_cas_list) > 1: col_status = f"{input_col_name}(原始 CAS 无效/缺失, PubChem 提供多个: {pubchem_cas_list})"
                            else: col_status = f"{input_col_name}(原始 CAS 无效/缺失, PubChem 未提供)"
                        if col_status: current_row_status_parts.append(col_status)
                    # Synonyms ('Alias') Handling
                    elif pubchem_attr_name == 'synonyms':
                         if isinstance(original_value, str) and not is_nan_or_none(original_value):
                             if found_compound.synonyms:
                                 specific_alias_used = None
                                 if found_by and found_by.startswith(alias_col): specific_alias_used = found_by.split('(')[-1].split(')')[0].strip("'")
                                 if specific_alias_used and original_value.strip() == specific_alias_used: current_row_status_parts.append(f"{input_col_name}(与匹配别名一致)")
                                 elif original_value.strip() in found_compound.synonyms: current_row_status_parts.append(f"{input_col_name}(存在于同义词中)")
                                 else: current_row_status_parts.append(f"{input_col_name}(不在同义词中)")
                             else: current_row_status_parts.append(f"{input_col_name}(PubChem无同义词)")
                    # Other Attributes Handling
                    else:
                        pubchem_value = getattr(found_compound, pubchem_attr_name, None)
                        if pubchem_value is None: continue
                        is_name_attribute = pubchem_attr_name == 'iupac_name'; needs_float = 'weight' in pubchem_attr_name or 'mass' in pubchem_attr_name or 'xlogp' in pubchem_attr_name or 'tpsa' in pubchem_attr_name; needs_int = 'count' in pubchem_attr_name or 'charge' in pubchem_attr_name
                        comparison_result_consistent = False
                        try:
                            if is_nan_or_none(original_value): update_needed = True; value_to_update = pubchem_value
                            elif needs_float:
                                p_float = clean_for_float(pubchem_value); o_float = clean_for_float(original_value)
                                if p_float is not None and o_float is not None and math.isclose(o_float, p_float, rel_tol=1e-5): comparison_result_consistent = True
                                elif p_float is not None: update_needed = True; value_to_update = p_float
                            elif needs_int:
                                try:
                                    p_int = int(float(pubchem_value)); o_int = int(float(original_value))
                                    if p_int == o_int: comparison_result_consistent = True
                                    else: update_needed = True; value_to_update = p_int
                                except (ValueError, TypeError):
                                    if str(original_value).strip() == str(pubchem_value).strip(): comparison_result_consistent = True
                                    else: update_needed = True; value_to_update = pubchem_value
                            else: # Default string comparison
                                if str(original_value).strip() == str(pubchem_value).strip(): comparison_result_consistent = True
                                else: update_needed = True; value_to_update = pubchem_value
                            # Override update decision for name column
                            if is_name_attribute:
                                if comparison_result_consistent: current_row_status_parts.append(f"{input_col_name}(与IUPAC名一致)")
                                else: current_row_status_parts.append(f"{input_col_name}(与IUPAC名不一致)")
                                if not (is_nan_or_none(original_value) or str(original_value).strip() == ''): update_needed = False # Prevent update
                        except Exception as comp_err: logging.warning(f"警告: 行 {index + 2}, 列 '{input_col_name}': 比较时出错 - {comp_err}"); current_row_status_parts.append(f'{input_col_name}(比较错误)'); error_indices.add(index)
                    # Perform Update if needed
                    if update_needed:
                        df.loc[index, input_col_name] = value_to_update
                        if input_col_name != pubchem_id_col: updates_made_cols.append(input_col_name); updated_indices.add(index); row_updated_flag = True

            # Determine Overall Status based on comparison results
            match_prefix = f"匹配成功 (通过 {found_by})"
            if updates_made_cols: # Check if any updates were made (includes PubChem ID)
                update_list_str = ', '.join(updates_made_cols)
                update_suffix = f"已更新: {update_list_str}"
                overall_status = f"{match_prefix}; {update_suffix}"
                other_statuses = [s for s in current_row_status_parts if not s.startswith(tuple([f"{col}(" for col in updates_made_cols]))]
                if other_statuses: overall_status += f"; {'; '.join(other_statuses)}"
            elif current_row_status_parts: overall_status = f"{match_prefix}; {'; '.join(current_row_status_parts)}"
            else: overall_status = f"{match_prefix}; 数据一致"; consistent_rows_count += 1

        # --- Step 5: Handle cases where no unique compound was found ---
        else:
            if overall_status == "未处理": overall_status = "未找到唯一匹配 (尝试名称/别名/ID)"
            # Add to no_match_indices if applicable
            if "未找到" in overall_status and "多个匹配" not in overall_status and "交叉验证" not in overall_status: # Exclude CV rejects from no match count
                no_match_indices.add(index)
            # multi_match_indices is already handled during lookup steps

        # Update the status column for the row
        df.loc[index, status_column] = overall_status

        # --- Log Optimization: Log only non-consistent statuses --- <<< MODIFIED >>>
        if not overall_status.endswith("数据一致"):
            logging.info(f"行 {index + 2}: {overall_status}") # Use logging.info

        # --- Log Optimization: Update counters ---
        # updated_indices and error_indices are updated when updates/errors occur
        # multi_match_indices is updated during lookup

    # --- End of Main Checking Loop ---

    logging.info("\nPubChem 数据核对与更新完成。")
    logging.info("\n--- 处理结果摘要 ---")
    logging.info(f"总处理行数: {processed_rows}")
    logging.info(f"数据一致行数: {consistent_rows_count}")
    logging.info(f"数据更新行数: {len(updated_indices)}")
    logging.info(f"报告交叉验证失败行数: {cross_validation_failures_reported}") # Added counter
    logging.info(f"未找到唯一匹配行数 (名称/别名/ID): {len(no_match_indices)}")
    logging.info(f"找到多个匹配行数 (名称/别名): {len(multi_match_indices)}")
    logging.info(f"处理出错行数: {len(error_indices)}")
    logging.info("--------------------")

    # --- Save the detailed results (without status column) ---
    try:
        df_detailed_output = df.copy()
        if status_column in df_detailed_output.columns:
            df_detailed_output = df_detailed_output.drop(columns=[status_column])
        df_detailed_output.to_excel(output_file_detailed, index=False, engine='openpyxl')
        logging.info(f"\n详细核对数据 (无状态列) 已成功保存到 '{output_file_detailed}'")
    except ImportError: logging.error(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_detailed}'。")
    except Exception as e: logging.error(f"保存详细结果文件 '{output_file_detailed}' 时出错: {e}")

    # --- Create and save the summary file (with status column) --- << REMOVED >>
    # logging.info(f"\n正在创建摘要文件...")
    # name_column_actual = find_name_column(df.columns)
    # summary_columns = []
    # # Add columns if they exist in the final DataFrame 'df'
    # if pubchem_id_col in df.columns: summary_columns.append(pubchem_id_col)
    # else: logging.warning(f"警告: PubChem ID 列 '{pubchem_id_col}' 未在最终数据中找到，无法添加到摘要。")

    # summary_name_col_added = False
    # if name_column_actual and name_column_actual in df.columns:
    #     summary_columns.append(name_column_actual); summary_name_col_added = True
    # elif name_col in df.columns: # Fallback to configured name_col
    #         summary_columns.append(name_col); summary_name_col_added = True
    #         logging.warning(f"警告: 未能在映射中找到与'iupac_name'匹配的列，摘要文件将使用配置的名称列 '{name_col}'。")
    # if not summary_name_col_added: logging.warning(f"警告: 未能找到分子名称列 ('{name_col}' 或通过映射)，摘要文件将不包含名称列。")

    # if status_column in df.columns: summary_columns.append(status_column)
    # else: logging.warning(f"警告: 状态列 '{status_column}' 未在最终数据中找到，无法添加到摘要。")


    # if len(summary_columns) >= 2:
    #     try:
    #         summary_df = df[summary_columns].copy()
    #         summary_df.to_excel(output_file_summary, index=False, engine='openpyxl')
    #         logging.info(f"摘要文件已成功保存到 '{output_file_summary}'")
    #     except ImportError: logging.error(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_summary}'。")
    #     except KeyError as e: logging.error(f"错误: 创建摘要时列名不存在: {e}。摘要文件未创建。")
    #     except Exception as e: logging.error(f"保存摘要文件 '{output_file_summary}' 时出错: {e}")
    # else: logging.error("错误：无法确定摘要文件的足够列，未创建摘要文件。")

