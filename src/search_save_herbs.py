from tcmsp import TcmspSpider
import logging
import os
from pathlib import Path

# --- Setup Logging ---
# Note: This logger is independent of the main run.py logger
# Consider passing logger instance if more integrated logging is needed
log_file_path = Path(__file__).parent / 'log' / 'search_save_herbs.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- Modified Function ---
def download_herb_data(herb_list_file):
    """
    Downloads data for herbs listed in the file.
    Args:
        herb_list_file (str or Path): Path to the text file containing herb names.
    Returns:
        list: A list of Path objects for successfully downloaded ingredient files.
    """
    tcmsp = TcmspSpider()
    downloaded_ingredient_files = []

    # Read herb list from the specified file
    herb_list = []
    try:
        with open(herb_list_file, "r", encoding="utf-8") as f:
            for line in f:
                herb = line.strip()
                if herb:
                    herb_list.append(herb)
    except FileNotFoundError:
        logger.error(f"Herb list file not found: {herb_list_file}")
        return []
    except Exception as e:
        logger.error(f"Error reading herb list file {herb_list_file}: {e}")
        return []

    logger.info(f"Found {len(herb_list)} herbs to query in '{herb_list_file}'.")

    tcmsp.token = tcmsp.get_token()
    if not tcmsp.token:
        logger.error("Failed to get TCMSP token. Aborting download.")
        return []

    # Ensure spider data directory exists (relative to tcmsp.py location)
    spider_data_path = Path(tcmsp.spider_file_path)
    spider_data_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Ensured spider data directory exists: {spider_data_path}")

    # Iterate through herbs
    for herb in herb_list:
        try:
            herb_three_names = tcmsp.get_herb_name(herb)

            if not herb_three_names:
                logger.warning(f"No information found for herb: {herb}")
                continue

            # If multiple matches, download for each
            for name_info in herb_three_names:
                herb_cn_name = name_info.get("herb_cn_name")
                herb_en_name = name_info.get("herb_en_name")
                herb_pinyin_name = name_info.get("herb_pinyin")

                if not all([herb_cn_name, herb_en_name, herb_pinyin_name]):
                    logger.warning(f"Incomplete name information for a match of '{herb}': {name_info}")
                    continue

                # Construct expected output file path to check later
                expected_ingredient_file = spider_data_path / f"{herb_pinyin_name}_ingredients.xlsx"

                logger.info(f"Processing: CN='{herb_cn_name}', EN='{herb_en_name}', Pinyin='{herb_pinyin_name}'")
                tcmsp.get_herb_data(herb_cn_name, herb_en_name, herb_pinyin_name)

                # Check if the ingredient file was actually created by get_herb_data
                if expected_ingredient_file.exists():
                     logger.info(f"Successfully downloaded and saved: {expected_ingredient_file.name}")
                     if expected_ingredient_file not in downloaded_ingredient_files:
                          downloaded_ingredient_files.append(expected_ingredient_file)
                else:
                     logger.warning(f"Ingredient file was expected but not found after download attempt: {expected_ingredient_file.name}")

        except Exception as e:
            logger.error(f"An error occurred while processing herb '{herb}': {e}", exc_info=True)

    logger.info(f"Download process finished. Successfully saved {len(downloaded_ingredient_files)} ingredient files.")
    return downloaded_ingredient_files


# --- Original Main Execution Block (modified to use the function) ---
if __name__ == "__main__":
    default_list_file = Path(__file__).resolve().parents[1] / 'herb_list.txt'
    logger.info(f"Running search_save_herbs.py directly.")
    logger.info(f"Looking for herb list at: {default_list_file}")

    if not default_list_file.exists():
         logger.error(f"Default herb list '{default_list_file}' not found when running script directly.")
    else:
        downloaded_files = download_herb_data(default_list_file)
        logger.info(f"\n--- Direct Script Execution Summary ---")
        if downloaded_files:
            logger.info("Downloaded ingredient files:")
            for f_path in downloaded_files:
                logger.info(f" - {f_path}")
        else:
            logger.info("No ingredient files were downloaded or saved successfully.")
        logger.info("--- End of Direct Execution ---")
