import pandas as pd
import os

# --- Dependencies ---
# This script requires the 'openpyxl' library to read/write .xlsx files.
# If you don't have it installed, run: pip install openpyxl

# --- Configuration ---
# Define the file names for the input Excel tables and the output tables.
# Please make sure these files exist in the same directory as the script,
# or provide the full path to the files.
file1 = 'table1.xlsx'
file2 = 'table2.xlsx'
output_file_merged = 'merged_table.xlsx' # Output for the full merged table
output_file_pubchem = 'merged_table_with_pubchem_id.xlsx' # Output for rows with PubChem ID

# Define the column name to merge on.
merge_column = 'molecule_name'
# Define the PubChem ID column name (ensure this matches the actual column name in table1)
pubchem_id_column = 'PubChem_id'

# --- Function to load data ---
def load_data(filename):
    """
    Loads data from an Excel file (.xlsx) into a pandas DataFrame.
    Args:
        filename (str): The path to the Excel file.
    Returns:
        pandas.DataFrame: The loaded DataFrame, or None if the file is not found, empty, or an error occurs.
    """
    if not os.path.exists(filename):
        print(f"错误：文件 '{filename}' 未找到。请确保文件存在于正确的目录中。")
        return None
    try:
        # Use read_excel for .xlsx files
        df = pd.read_excel(filename, engine='openpyxl')
        if df.empty:
            print(f"警告：文件 '{filename}' 为空。")
        return df
    except ImportError:
        print("错误：缺少 'openpyxl' 库。请先通过 'pip install openpyxl' 安装它。")
        return None
    except Exception as e:
        print(f"加载文件 '{filename}' 时出错: {e}")
        return None

# --- Main script ---
if __name__ == "__main__":
    print("重要提示：此脚本需要 'openpyxl' 库来处理 Excel 文件。")
    print("如果尚未安装，请在终端运行: pip install openpyxl\n")

    print(f"正在加载第一个表格: {file1}")
    df1 = load_data(file1)

    print(f"正在加载第二个表格: {file2}")
    df2 = load_data(file2)

    # Proceed only if both DataFrames were loaded successfully
    if df1 is not None and df2 is not None:
        # --- Data Cleaning Step ---
        # Check if the merge column exists before attempting to clean it.
        if merge_column in df1.columns and merge_column in df2.columns:
            print(f"\n正在清理 '{merge_column}' 列中的前后空格...")
            # Convert column to string type first to avoid errors with non-string data, then strip whitespace.
            df1[merge_column] = df1[merge_column].astype(str).str.strip()
            df2[merge_column] = df2[merge_column].astype(str).str.strip()
            print("空格清理完成。")
        else:
            if merge_column not in df1.columns:
                print(f"错误：合并列 '{merge_column}' 在文件 '{file1}' 中未找到。无法进行清理。")
                print(f"'{file1}' 中的可用列: {list(df1.columns)}")
            if merge_column not in df2.columns:
                print(f"错误：合并列 '{merge_column}' 在文件 '{file2}' 中未找到。无法进行清理。")
                print(f"'{file2}' 中的可用列: {list(df2.columns)}")
            print("\n由于合并列缺失，无法继续执行合并操作。")
            exit()

        # --- Merging Step ---
        print(f"\n正在以 '{merge_column}' 列为基准合并表格...")
        try:
            merged_df = pd.merge(df1, df2, on=merge_column, how='inner')

            print(f"\n合并完成。")
            print(f"原始表格1行数 (清理后): {len(df1)}")
            print(f"原始表格2行数 (清理后): {len(df2)}")
            print(f"合并后表格总行数: {len(merged_df)}")
            print(f"合并后表格列数: {len(merged_df.columns)}")
            print("\n合并后表格的前5行:")
            print(merged_df.head())

            # --- Save the full merged DataFrame ---
            try:
                merged_df.to_excel(output_file_merged, index=False, engine='openpyxl')
                print(f"\n完整的合并表格已成功保存到 '{output_file_merged}'")
            except ImportError:
                 print(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_merged}'。请先通过 'pip install openpyxl' 安装它。")
            except Exception as e:
                print(f"保存文件 '{output_file_merged}' 时出错: {e}")

            # --- Filter and save rows with PubChem ID ---
            # Check if the PubChem ID column exists in the merged DataFrame
            if pubchem_id_column in merged_df.columns:
                print(f"\n正在筛选 '{pubchem_id_column}' 列有值的行...")
                # Filter rows where PubChem_id is not NaN/NaT and not an empty string after converting to string
                # .astype(str) ensures comparison works even if IDs are numeric
                pubchem_filtered_df = merged_df[
                    merged_df[pubchem_id_column].notna() & \
                    (merged_df[pubchem_id_column].astype(str).str.strip() != '')
                ].copy() # Use .copy() to avoid SettingWithCopyWarning

                print(f"筛选完成。找到 {len(pubchem_filtered_df)} 行包含有效的 '{pubchem_id_column}'。")

                if not pubchem_filtered_df.empty:
                    try:
                        pubchem_filtered_df.to_excel(output_file_pubchem, index=False, engine='openpyxl')
                        print(f"包含 PubChem ID 的行已成功保存到 '{output_file_pubchem}'")
                    except ImportError:
                        print(f"错误：缺少 'openpyxl' 库。无法保存 Excel 文件 '{output_file_pubchem}'。请先通过 'pip install openpyxl' 安装它。")
                    except Exception as e:
                        print(f"保存文件 '{output_file_pubchem}' 时出错: {e}")
                else:
                    print(f"未找到包含有效 '{pubchem_id_column}' 的行，因此未创建文件 '{output_file_pubchem}'。")
            else:
                print(f"\n警告：列 '{pubchem_id_column}' 在合并后的表格中未找到。无法筛选并保存 PubChem ID 子集。")
                print(f"合并后表格的列: {list(merged_df.columns)}")


        except Exception as e:
            print(f"处理表格时出错: {e}")
    else:
        print("\n由于一个或多个文件加载失败，无法执行后续操作。")

