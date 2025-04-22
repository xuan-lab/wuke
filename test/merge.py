import pandas as pd
import os
import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime
import re
from typing import Optional, List, Tuple # Added for type hinting

# --- Dependencies ---
# This script requires 'openpyxl' to read/write .xlsx files: pip install openpyxl

# --- Constants ---
# Define the default column name to merge on. Can be overridden by CLI argument.
DEFAULT_MERGE_COLUMN = 'molecule_name'
# Define the default PubChem ID column name. Can be overridden by CLI argument.
DEFAULT_PUBCHEM_ID_COLUMN = 'PubChem_id'
# Define the name for the temporary normalized column used for matching
NORMALIZED_NAME_COLUMN = '_normalized_name_for_merge'
# Suffixes used during merge if column names clash (other than the merge key)
SUFFIX_FILE1 = '_file1'
SUFFIX_FILE2 = '_file2'

# --- Setup Logging ---
# Determine script directory robustly
try:
    # Works when run as a script
    script_dir = Path(__file__).parent.resolve()
except NameError:
    # Fallback for interactive environments (like Jupyter)
    script_dir = Path('.').resolve()

log_dir_path = script_dir / 'log'
output_dir_path = script_dir / 'merge_file' # Output directory

# Create directories if they don't exist
try:
    log_dir_path.mkdir(parents=True, exist_ok=True)
    output_dir_path.mkdir(parents=True, exist_ok=True)
except OSError as e:
    print(f"错误：无法创建目录 ('{log_dir_path}' or '{output_dir_path}'): {e}")
    sys.exit(1)

# Configure logging
log_file_path = log_dir_path / f'merge_log_{datetime.now():%Y%m%d_%H%M%S}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'), # Log to file
        logging.StreamHandler(sys.stdout) # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# --- Function to load data ---
def load_data(filename: str) -> Optional[pd.DataFrame]:
    """
    Loads data from an Excel file (.xlsx) into a pandas DataFrame.

    Args:
        filename (str): The path to the Excel file.

    Returns:
        Optional[pd.DataFrame]: The loaded DataFrame, or None if an error occurs.
    """
    if not Path(filename).is_file():
        logger.error(f"文件 '{filename}' 未找到或不是一个有效文件。")
        return None
    if not filename.lower().endswith('.xlsx'):
         logger.error(f"输入文件 '{filename}' 必须是 .xlsx 格式。")
         return None
    try:
        df = pd.read_excel(filename, engine='openpyxl')
        if df.empty:
            logger.warning(f"文件 '{filename}' 为空。")
        else:
            logger.info(f"成功加载文件 '{filename}'，包含 {len(df)} 行和 {len(df.columns)} 列。")
        # Ensure merge column exists early
        # Note: Merge column name is checked later in main logic after parsing args
        return df
    except ImportError:
        logger.error("错误：缺少 'openpyxl' 库。请通过 'pip install openpyxl' 安装它。")
        return None
    except Exception as e:
        logger.error(f"加载文件 '{filename}' 时出错: {e}")
        return None

# --- Function for Vectorized Normalization ---
def normalize_column_vectorized(series: pd.Series) -> pd.Series:
    """
    Normalizes a pandas Series for strict matching using vectorized operations.
    Converts to lowercase, removes punctuation/symbols, standardizes whitespace.

    Args:
        series (pd.Series): The input Series (typically the merge column).

    Returns:
        pd.Series: The normalized Series. Empty strings or NaNs become None.
    """
    if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
         logger.warning(f"尝试规范化的列不是字符串或对象类型，可能导致错误。列类型: {series.dtype}")
         # Attempt conversion, coercing errors to NaN
         series = series.astype(str, errors='ignore')


    # Convert to string and lowercase
    normalized = series.astype(str).str.lower()
    # Remove punctuation and symbols (keeps letters, numbers, whitespace)
    normalized = normalized.str.replace(r'[^\w\s]', '', regex=True)
    # Replace multiple whitespace characters with a single space and strip ends
    normalized = normalized.str.replace(r'\s+', ' ', regex=True).str.strip()
    # Replace empty strings resulting from normalization with None
    normalized = normalized.replace('', None)
    return normalized

# --- Function to Prepare DataFrame for Merging ---
def prepare_for_merge(df: pd.DataFrame, merge_col: str, df_name: str) -> pd.DataFrame:
    """
    Validates merge column, normalizes it, checks for duplicates, and cleans the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        merge_col (str): The name of the column to merge on.
        df_name (str): A descriptive name for the DataFrame (e.g., 'File 1').

    Returns:
        pd.DataFrame: The prepared DataFrame with normalized column, or an empty DataFrame if validation fails.
    """
    if merge_col not in df.columns:
        logger.error(f"合并列 '{merge_col}' 在 {df_name} 中未找到。可用列: {list(df.columns)}")
        return pd.DataFrame() # Return empty DataFrame on critical error

    logger.info(f"正在对 {df_name} 的 '{merge_col}' 列进行规范化...")
    df[NORMALIZED_NAME_COLUMN] = normalize_column_vectorized(df[merge_col])

    # Check for rows that became None after normalization
    invalid_count = df[NORMALIZED_NAME_COLUMN].isna().sum()
    if invalid_count > 0:
        logger.warning(f"在 {df_name} 中，有 {invalid_count} 行的 '{merge_col}' 在规范化后变为空或无效，将无法匹配。")

    # Drop rows where the normalized name is None
    df_cleaned = df.dropna(subset=[NORMALIZED_NAME_COLUMN]).copy()
    logger.info(f"{df_name} 规范化后有效行数: {len(df_cleaned)}")

    # Check for duplicates in the normalized column *within* this DataFrame
    duplicates = df_cleaned[df_cleaned.duplicated(subset=[NORMALIZED_NAME_COLUMN], keep=False)]
    if not duplicates.empty:
        logger.warning(f"{df_name} 中发现 {len(duplicates)} 行在规范化后具有重复的名称 ('{NORMALIZED_NAME_COLUMN}')。")
        logger.warning(f"重复的规范化名称示例: {duplicates[NORMALIZED_NAME_COLUMN].unique()[:5]}")
        # Strategy: Keep the first occurrence, drop others
        logger.warning("将保留每个重复规范化名称的第一个匹配行，并删除后续重复行。")
        df_cleaned = df_cleaned.drop_duplicates(subset=[NORMALIZED_NAME_COLUMN], keep='first')
        logger.info(f"{df_name} 去除规范化名称重复项后剩余行数: {len(df_cleaned)}")

    return df_cleaned

# --- Function to Merge DataFrames ---
def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two prepared DataFrames based on the normalized name column.

    Args:
        df1 (pd.DataFrame): The first prepared DataFrame.
        df2 (pd.DataFrame): The second prepared DataFrame.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    if df1.empty or df2.empty:
        logger.warning("一个或两个 DataFrame 为空，无法执行合并。")
        return pd.DataFrame()

    logger.info(f"正在以规范化后的名称 ('{NORMALIZED_NAME_COLUMN}') 为基准合并表格 (inner join)...")
    try:
        # Identify columns unique to df2 before merge (excluding the merge key)
        df2_unique_cols = [col for col in df2.columns if col not in df1.columns and col != NORMALIZED_NAME_COLUMN]
        cols_to_use_from_df2 = [NORMALIZED_NAME_COLUMN] + df2_unique_cols

        merged_df = pd.merge(
            df1,
            df2[cols_to_use_from_df2], # Select only merge key and unique cols from df2
            on=NORMALIZED_NAME_COLUMN,
            how='inner', # Ensures only exact matches (post-normalization) are kept
            # No suffixes needed now as we selected unique columns from df2
        )

        logger.info("合并完成。")
        logger.info(f"原始表格1有效行数 (去重后): {len(df1)}")
        logger.info(f"原始表格2有效行数 (去重后): {len(df2)}")
        logger.info(f"合并后表格总行数: {len(merged_df)}")

        # --- Clean up columns after merge ---
        # Drop the temporary normalized column used for merging
        if NORMALIZED_NAME_COLUMN in merged_df.columns:
            merged_df = merged_df.drop(columns=[NORMALIZED_NAME_COLUMN])
            logger.info(f"已移除用于合并的临时列 '{NORMALIZED_NAME_COLUMN}'。")

        logger.info(f"最终合并后表格列数: {len(merged_df.columns)}")
        logger.info(f"最终列名: {list(merged_df.columns)}")
        if not merged_df.empty:
             logger.info("\n合并后表格的前5行:")
             # Use try-except for to_string in case of rare display issues
             try:
                 logger.info("\n" + merged_df.head().to_string())
             except Exception as e:
                 logger.warning(f"无法打印合并后表格的 head: {e}")
        else:
             logger.info("合并结果为空。")

        return merged_df

    except Exception as e:
        logger.error(f"合并表格时出错: {e}", exc_info=True) # Log traceback
        return pd.DataFrame()

# --- Function to Save DataFrame ---
def save_dataframe(df: pd.DataFrame, filename: Path, sheet_name: str = 'Sheet1'):
    """
    Saves a DataFrame to an Excel file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (Path): The output file path.
        sheet_name (str): The name of the sheet in the Excel file.
    """
    if df.empty:
        logger.info(f"DataFrame 为空，跳过保存到 '{filename}'。")
        return

    logger.info(f"正在保存 DataFrame ({len(df)} 行) 到 '{filename}'...")
    try:
        df.to_excel(filename, index=False, engine='openpyxl', sheet_name=sheet_name)
        logger.info(f"DataFrame 已成功保存到 '{filename}'")
    except ImportError:
        logger.error(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{filename}'。请通过 'pip install openpyxl' 安装它。")
    except Exception as e:
        logger.error(f"保存文件 '{filename}' 时出错: {e}")

# --- Main script ---
def main():
    logger.info("--- 合并脚本开始 (严格规范化匹配模式) ---")
    logger.info(f"日志文件: {log_file_path}")
    logger.info(f"输出目录: {output_dir_path}")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description='使用严格规范化匹配合并两个 Excel 表格 (.xlsx)，并可选择性筛选包含 PubChem ID 的行。',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show default values in help
    )
    parser.add_argument('input_file1', type=str, help='第一个输入的 Excel 文件路径 (.xlsx)')
    parser.add_argument('input_file2', type=str, help='第二个输入的 Excel 文件路径 (.xlsx)')
    parser.add_argument(
        '--merge_col',
        type=str,
        default=DEFAULT_MERGE_COLUMN,
        help='用于合并的列名。'
    )
    parser.add_argument(
        '--pubchem_col',
        type=str,
        default=DEFAULT_PUBCHEM_ID_COLUMN,
        help=f'第一个文件中包含 PubChem ID 的列名 (用于最终筛选)。'
    )

    # Check if any arguments were passed, provide help if not
    if len(sys.argv) < 3: # Need script name + 2 positional arguments
        parser.print_help(sys.stderr)
        logger.error("错误：缺少必需的输入文件参数。")
        sys.exit(1)

    args = parser.parse_args()
    file1_path = args.input_file1
    file2_path = args.input_file2
    merge_column_name = args.merge_col
    pubchem_id_column_name = args.pubchem_col

    # --- Derive Output Filenames ---
    base_name1 = Path(file1_path).stem # Get filename without extension
    ext1 = Path(file1_path).suffix # Get extension

    # Construct output paths using pathlib
    output_file_merged = output_dir_path / f"{base_name1}_merged_strict{ext1}"
    output_file_pubchem = output_dir_path / f"{base_name1}_with_pubchem_id_strict{ext1}"

    logger.info(f"第一个输入文件: {file1_path}")
    logger.info(f"第二个输入文件: {file2_path}")
    logger.info(f"合并列: '{merge_column_name}'")
    logger.info(f"PubChem ID 列: '{pubchem_id_column_name}'")
    logger.info(f"合并输出文件: {output_file_merged}")
    logger.info(f"筛选后输出文件: {output_file_pubchem}")

    # --- Load Data ---
    logger.info(f"正在加载第一个表格: {file1_path}")
    df1 = load_data(file1_path)

    logger.info(f"正在加载第二个表格: {file2_path}")
    df2 = load_data(file2_path)

    # Proceed only if both DataFrames were loaded successfully
    if df1 is None or df2 is None:
        logger.error("由于一个或多个文件加载失败，无法执行后续操作。")
        sys.exit(1)

    # --- Prepare DataFrames ---
    df1_prepared = prepare_for_merge(df1, merge_column_name, f"文件 '{Path(file1_path).name}'")
    df2_prepared = prepare_for_merge(df2, merge_column_name, f"文件 '{Path(file2_path).name}'")

    # --- Merge DataFrames ---
    merged_df = merge_dataframes(df1_prepared, df2_prepared)

    # --- Save Full Merged Result ---
    if not merged_df.empty:
        save_dataframe(merged_df, output_file_merged, sheet_name='Merged_Strict')
    else:
        logger.warning("合并结果为空，未保存完整合并文件。")


    # --- Filter and save rows with PubChem ID ---
    if not merged_df.empty:
        # Check if the PubChem ID column (from the *original* df1) exists in the final merged df
        if pubchem_id_column_name in merged_df.columns:
            logger.info(f"正在筛选 '{pubchem_id_column_name}' 列有值的行...")

            # Filter rows where PubChem ID is not NaN and not an empty string after stripping
            pubchem_filtered_df = merged_df[
                merged_df[pubchem_id_column_name].notna() &
                (merged_df[pubchem_id_column_name].astype(str).str.strip() != '')
            ].copy()

            logger.info(f"筛选完成。找到 {len(pubchem_filtered_df)} 行包含有效的 '{pubchem_id_column_name}'。")

            # Save the filtered DataFrame
            save_dataframe(pubchem_filtered_df, output_file_pubchem, sheet_name='Merged_With_PubChemID')
            # Conditional print for external tools if needed
            if not pubchem_filtered_df.empty:
                 print(f"MERGE_PUBCHEM_OUTPUT:{output_file_pubchem}") # Keep this if an external process relies on it

        else:
            logger.warning(f"列 '{pubchem_id_column_name}' 在最终合并的表格中未找到。无法筛选并保存 PubChem ID 子集。")
            logger.warning(f"合并后表格的可用列: {list(merged_df.columns)}")
    else:
        logger.info("由于合并结果为空，跳过 PubChem ID 筛选步骤。")


    logger.info("--- 合并脚本结束 ---")

if __name__ == "__main__":
    main()

