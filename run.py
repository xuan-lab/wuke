import sys
import os
import subprocess
import logging
import argparse
from pathlib import Path
import time

# --- Configuration ---
SRC_DIR = Path(__file__).parent / 'src'
DATA_DIR = Path(__file__).parent / 'data'
SPIDER_DATA_DIR = DATA_DIR / 'spider_data'
MERGE_OUTPUT_DIR = SRC_DIR / 'merge_file' # Relative to src/merge.py location
VERIFIED_OUTPUT_DIR = SRC_DIR / 'verified_file' # Relative to src/verified.py location
LOG_DIR = Path(__file__).parent / 'logs' # Central log directory at root

# Ensure required directories exist
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
SPIDER_DATA_DIR.mkdir(exist_ok=True)
# merge.py and verified.py create their own output dirs relative to their location

# --- Logging Setup ---
log_file = LOG_DIR / 'run_workflow.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Also print logs to console
    ]
)

# --- Helper Function ---
def run_script(script_path, args, cwd=None):
    """Runs a python script using subprocess and logs output."""
    command = [sys.executable, str(script_path)] + args
    logging.info(f"Running command: {' '.join(command)}")
    try:
        # Use UTF-8 encoding for subprocess output
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Redirect stderr to stdout
            text=True,
            encoding='utf-8',
            errors='replace', # Replace characters that cannot be decoded
            cwd=cwd or Path(__file__).parent # Run from project root by default
        )

        output_lines = []
        while True:
            line = process.stdout.readline()
            if not line:
                break
            line = line.strip()
            if line: # Avoid logging empty lines
                logging.info(f"  [{script_path.name}] {line}")
                output_lines.append(line)

        process.wait()
        if process.returncode != 0:
            logging.error(f"Script {script_path.name} failed with return code {process.returncode}")
            return None, process.returncode
        else:
            logging.info(f"Script {script_path.name} completed successfully.")
            return "\n".join(output_lines), 0 # Return combined output
    except FileNotFoundError:
        logging.error(f"Error: Python executable not found at {sys.executable} or script not found at {script_path}")
        return None, 1
    except Exception as e:
        logging.error(f"An unexpected error occurred while running {script_path.name}: {e}")
        return None, 1

# --- Main Workflow Logic ---
def main(herb_list_path, tcmbank_path):
    logging.info("--- Starting Workflow ---")
    logging.info(f"Herb List File: {herb_list_path}")
    logging.info(f"TCMSP Bank File: {tcmbank_path}")

    # --- Step 1: Download Herb Data ---
    logging.info("\n--- Step 1: Downloading Herb Data ---")
    search_script = SRC_DIR / 'search_save_herbs.py'
    if Path(herb_list_path).name != 'herb_list.txt' or Path(herb_list_path).parent != Path(__file__).parent:
         logging.warning(f"search_save_herbs.py expects 'herb_list.txt' in the root directory. Copying selected list there.")
         try:
             import shutil
             shutil.copy2(herb_list_path, Path(__file__).parent / 'herb_list.txt')
         except Exception as e:
             logging.error(f"Failed to copy herb list: {e}")
             return

    start_time = time.time()
    search_output, search_rc = run_script(search_script, [])
    if search_rc != 0:
        logging.error("Failed to download herb data. Aborting.")
        return
    logging.info(f"Herb download step finished in {time.time() - start_time:.2f} seconds.")

    downloaded_files = list(SPIDER_DATA_DIR.glob("*_ingredients.xlsx"))
    if not downloaded_files:
        logging.warning("No ingredient files found in data/spider_data/. Check download step logs.")
        logging.info("Proceeding with any existing ingredient files found in data/spider_data/.")

    # --- Step 2 & 3: Merge and Verify for each downloaded file ---
    merge_script = SRC_DIR / 'merge.py'
    verify_script = SRC_DIR / 'verified.py'

    if not Path(tcmbank_path).exists():
        logging.error(f"TCMSP Bank file not found: {tcmbank_path}. Cannot proceed with merge/verify.")
        return

    successful_merges = []
    failed_merges = []
    successful_verifications_pubchem = []
    successful_verifications_full = []
    failed_verifications = []

    for ingredient_file in downloaded_files:
        herb_name = ingredient_file.stem.replace('_ingredients', '')
        logging.info(f"\n--- Processing Herb: {herb_name} ---")
        logging.info(f"Ingredient file: {ingredient_file}")

        # --- Step 2: Merge ---
        logging.info(f"--- Step 2: Merging {herb_name} with TCMSP Bank ---")
        merge_args = [str(ingredient_file), str(tcmbank_path)]
        start_time = time.time()
        merge_output, merge_rc = run_script(merge_script, merge_args, cwd=SRC_DIR)

        merged_pubchem_file_path = None
        merged_full_file_path = None

        if merge_rc == 0:
            logging.info(f"Merge step for {herb_name} completed successfully.")
            successful_merges.append(herb_name)
            for line in merge_output.strip().split('\n'):
                 if line.startswith("MERGE_PUBCHEM_OUTPUT:"):
                     path_str = line.split(":", 1)[1].strip()
                     try:
                         resolved_path = (SRC_DIR / Path(path_str)).resolve()
                         if resolved_path.exists():
                             merged_pubchem_file_path = resolved_path
                             logging.info(f"Found PubChem filtered merge output: {merged_pubchem_file_path}")
                         else:
                              logging.warning(f"Merge script reported PubChem output '{path_str}', but file not found at {resolved_path}")
                     except Exception as e:
                         logging.warning(f"Could not parse PubChem file path from merge output line: {line}. Error: {e}")
                 elif line.startswith("MERGE_FULL_OUTPUT:"):
                     path_str = line.split(":", 1)[1].strip()
                     try:
                         resolved_path = (SRC_DIR / Path(path_str)).resolve()
                         if resolved_path.exists():
                             merged_full_file_path = resolved_path
                             logging.info(f"Found full merge output: {merged_full_file_path}")
                         else:
                              logging.warning(f"Merge script reported full output '{path_str}', but file not found at {resolved_path}")
                     except Exception as e:
                         logging.warning(f"Could not parse full file path from merge output line: {line}. Error: {e}")
        else:
            logging.error(f"Merge step failed for {herb_name}.")
            failed_merges.append(herb_name)
            continue

        # --- Step 3: Verify (Both Files) ---
        verification_failed_for_herb = False

        if merged_pubchem_file_path:
            logging.info(f"--- Step 3a: Verifying {herb_name} Data (PubChem ID Filtered) ---")
            verify_args_pubchem = [str(merged_pubchem_file_path)]
            start_time = time.time()
            verify_output_pubchem, verify_rc_pubchem = run_script(verify_script, verify_args_pubchem, cwd=SRC_DIR)

            if verify_rc_pubchem != 0:
                logging.error(f"Verification step failed for {herb_name} (PubChem ID Filtered file).")
                verification_failed_for_herb = True
            else:
                logging.info(f"Verification step for {herb_name} (PubChem ID Filtered file) completed successfully.")
                successful_verifications_pubchem.append(herb_name)
                verified_file_path_pubchem = VERIFIED_OUTPUT_DIR / f"{merged_pubchem_file_path.stem}_verified{merged_pubchem_file_path.suffix}"
                logging.info(f"Verified PubChem file should be at: {verified_file_path_pubchem}")

            logging.info(f"Verify step (PubChem) for {herb_name} finished in {time.time() - start_time:.2f} seconds.")
        else:
             logging.warning(f"Could not find the PubChem filtered output file from merge step for {herb_name}. Skipping verification 3a.")
             verification_failed_for_herb = True

        if merged_full_file_path:
            logging.info(f"--- Step 3b: Verifying {herb_name} Data (Full Merged) ---")
            verify_args_full = [str(merged_full_file_path)]
            start_time = time.time()
            verify_output_full, verify_rc_full = run_script(verify_script, verify_args_full, cwd=SRC_DIR)

            if verify_rc_full != 0:
                logging.error(f"Verification step failed for {herb_name} (Full Merged file).")
            else:
                logging.info(f"Verification step for {herb_name} (Full Merged file) completed successfully.")
                successful_verifications_full.append(herb_name)
                verified_file_path_full = VERIFIED_OUTPUT_DIR / f"{merged_full_file_path.stem}_verified{merged_full_file_path.suffix}"
                logging.info(f"Verified Full file should be at: {verified_file_path_full}")

            logging.info(f"Verify step (Full) for {herb_name} finished in {time.time() - start_time:.2f} seconds.")
        else:
             logging.warning(f"Could not find the full merged output file from merge step for {herb_name}. Skipping verification 3b.")

        if verification_failed_for_herb:
             if herb_name not in failed_verifications:
                 failed_verifications.append(herb_name)

    # --- Final Summary ---
    logging.info("\n--- Workflow Summary ---")
    logging.info(f"Successfully merged herbs: {', '.join(successful_merges) or 'None'}")
    logging.info(f"Successfully verified herbs (PubChem Filtered): {', '.join(successful_verifications_pubchem) or 'None'}")
    logging.info(f"Successfully verified herbs (Full Merged): {', '.join(successful_verifications_full) or 'None'}")
    if failed_merges:
        logging.error(f"Failed merges: {', '.join(failed_merges)}")
    if failed_verifications:
        logging.error(f"Failed verifications (at least one step): {', '.join(failed_verifications)}")

    logging.info("--- Workflow End ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Automated TCMSP Data Processing Workflow Runner')
    parser.add_argument('herb_list', type=str, help='Path to the herb list file (e.g., herb_list.txt)')
    parser.add_argument('tcmbank', type=str, help='Path to the TCMSP Bank Excel file (e.g., data/tcmbank.xlsx)')
    args = parser.parse_args()

    if not Path(args.herb_list).is_file():
        logging.error(f"Herb list file not found: {args.herb_list}")
        sys.exit(1)
    if not Path(args.tcmbank).is_file():
        logging.error(f"TCMSP Bank file not found: {args.tcmbank}")
        sys.exit(1)

    main(args.herb_list, args.tcmbank)
