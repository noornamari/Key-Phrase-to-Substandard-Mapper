import sys
import logging
import anthropic
import csv
import time
import json
import multiprocessing
import os
import gspread
from google.oauth2.service_account import Credentials


def setup_google_sheet(credentials_file, spreadsheet_id, sheet_name):
    """
    Sets up and returns a Google Sheet worksheet.

    Args:
        credentials_file (str): Path to the service account credentials JSON file.
        spreadsheet_id (str): ID of the Google Spreadsheet.
        sheet_name (str): Name of the worksheet within the spreadsheet.

    Returns:
        gspread.models.Worksheet: The specified worksheet object.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    return sheet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("key_phrase_mapper.log", encoding='utf-8')
    ]
)

# Define the schema for the Anthropic API tool use
schema = {
    "name": "getSubstandardKeyPhrases",
    "description": "Map key phrases to substandards and return the mapping",
    "input_schema": {
        "type": "object",
        "properties": {
            "scratchpad": {
                "type": "string",
                "description": "An area to note initial thoughts and the mapping process for each substandard"
            },
            "substandards": {
                "type": "object",
                "description": "A mapping of each substandard to its associated key phrases",
                "additionalProperties": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "description": "A key phrase associated with the substandard"
                    },
                    "description": "An array of key phrases for the substandard"
                }
            }
        },
        "required": [
            "scratchpad",
            "substandards"
        ]
    }
}

# Define the user prompt for mapping key phrases to substandards
user_prompt = """
You will be provided with two lists:

1. Substandards:
<substandards>
{SUBSTANDARDS}
</substandards>

2. Key Phrases:
<key_phrases>
{KEY_PHRASES}
</key_phrases>

Your task is to map each key phrase to the most relevant substandard, ensuring that all key phrases are used and each is mapped only once.

Follow these steps:

1. Carefully read and understand each substandard.
2. Examine each key phrase and determine which substandard it aligns with most closely.
3. Assign each key phrase to one substandard based on the highest relevance.
4. Ensure that each substandard receives at least one key phrase if possible.
5. If a substandard does not have any appropriate key phrases, pair it with an empty list.

Before providing your final answer, use a <scratchpad> to think through your mapping process. Consider the following:
- How each key phrase relates to the substandards
- Any challenges in mapping certain phrases
- Your reasoning for assigning phrases to specific substandards

Provide your response as a JSON object where each substandard is a key, and the value is a list of key phrases assigned to it. Each key phrase should be mapped, and each list should reflect the comprehensive use of all key phrases. The JSON object should also include a field for the "scratchpad"
Remember:
- Each key phrase should be used only once.
- All key phrases MUST be mapped.
- If a substandard has no relevant key phrases, it should have an empty list as its value.
- Ensure your JSON is properly formatted.
Begin your mapping process now.
"""

def analyze_output_dict(output):
    """
    Analyzes a mapping output dictionary to count items and check uniqueness.
    
    Args:
        output (dict): Dictionary mapping substandards to key phrases
        
    Returns:
        tuple: (total count, uniqueness boolean)
    """
    total = 0
    
    # Count items
    for value in output.values():
        if isinstance(value, list):
            total += len(value)

    # Check uniqueness
    seen_values = set()
    is_unique = True
    for value in output.values():
        hashable_value = tuple(value) if isinstance(value, list) else value
        if hashable_value in seen_values:
            is_unique = False
            break
        seen_values.add(hashable_value)
    
    return total, is_unique

def get_mapping(event, substandards, key_phrases, max_retries=5):
    """
    Communicates with the Anthropic API to map key phrases to substandards.

    Args:
        event (dict): Event dictionary containing configuration like API keys.
        substandards (list): List of substandards.
        key_phrases (list): List of key phrases.
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 5.

    Returns:
        dict or None: Mapping of substandards to key phrases along with scratchpad or None if failed.
    """
    claude_api_key = event.get("claude_api_key")

    claude_client = anthropic.Anthropic(api_key=claude_api_key)
    prompt = user_prompt
    user_message = prompt.format(
        SUBSTANDARDS=substandards,
        KEY_PHRASES=key_phrases
    )

    for retry_count in range(max_retries):
        try:
            response = claude_client.messages.create(
                model=event.get("model"),
                tools=[schema],
                temperature=event.get("temperature"),
                messages=[{"role": "user", "content": user_message}],
                max_tokens=8000,
            )
            
            for content in response.content:
                if content.type == "tool_use":
                    logging.info("Successfully got mapping result from Anthropic API")
                    return content.input
            logging.error("No tool use response found in Claude's output")
            return None
            
        except Exception as e:
            logging.error(f"Attempt {retry_count + 1} failed: {str(e)}")
            if retry_count < max_retries - 1:
                logging.info("Retrying after a short delay...")
                time.sleep(2)
            continue
    
    logging.error(f"Failed to process objective after {max_retries} attempts")
    return None

def process_objective(event, output_path, headers, lock, learning_objective, substandards, key_phrases):
    """
    Processes a single objective by mapping its key phrases to substandards and writing the result.

    Args:
        event (dict): Event dictionary containing configuration like API keys.
        output_path (str): Path to the output CSV file.
        headers (list): List of CSV headers.
        lock (multiprocessing.Lock): Lock object for thread-safe file writing.
        learning_objective (str): Learning objective.
        substandards (list): Substandards associated with the learning objective.
        key_phrases (list): Key phrases associated with the learning objective.
    """
    try:
        logging.debug(f"Processing ID: {learning_objective}")

        mapping_result = get_mapping(event, substandards, key_phrases)
        if mapping_result is None:
            logging.error(f"No valid mapping result for ID '{learning_objective}'")
            return

        logging.debug(f"Raw Mapping Result: {mapping_result}")

        # Extract 'scratchpad' and 'substandards' mapping
        scratchpad = mapping_result.get("scratchpad", "")
        substandards_mapping = mapping_result.get("substandards", {})

        if not isinstance(substandards_mapping, dict):
            logging.error(f"Unexpected format for 'substandards' in mapping result: {substandards_mapping}")
            logging.debug(f"Full Mapping Result: {mapping_result}")
            return
        
        total_count, is_unique = analyze_output_dict(substandards_mapping)
        row_data = {
            'Learning Objective': learning_objective,
            'Substandards': json.dumps(substandards, ensure_ascii=False),
            'Key Phrases': json.dumps(key_phrases, ensure_ascii=False),
            'Thinking': scratchpad,
            'Substandards to Key Phrases Mapping': json.dumps(substandards_mapping, ensure_ascii=False),
            'Number of Key Phrases': len(key_phrases),  
            "Total Key Phrases Mapped": total_count,
            "All Key Phrases Mapped Unique?": "Yes" if is_unique else "No"
        }
                
        with lock:
            try:
                with open(output_path, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers)
                    writer.writerow(row_data)
                    # logging.info(f"Successfully wrote mapping for ID '{learning_objective}' to {output_path}")
            except Exception as write_error:
                logging.error(f"Error writing to file for ID '{learning_objective}': {str(write_error)}")
                logging.error(f"Attempted to write to: {output_path}")
                
    except Exception as e:
        logging.error(f"Error processing mapping for ID '{learning_objective}': {repr(e)}")

def orchestrator(event):
    """
    Orchestrates the entire mapping process from reading inputs to writing outputs.

    Args:
        event (dict): Event dictionary containing configuration details like file names, Google Sheet info, etc.
    """
    logging.info("Starting main process")

    # Read inputs from Google Sheet or CSV file
    google_sheet_info = event.get("google_sheet")
    if google_sheet_info:
        logging.info("Setting up Google Sheets")
        # Setup Input Sheet
        input_sheet = setup_google_sheet(
            google_sheet_info["credentials_file"],
            google_sheet_info["spreadsheet_id"],
            google_sheet_info["input_sheet_name"]
        )
        # Setup Output Sheet
        output_sheet = setup_google_sheet(
            google_sheet_info["credentials_file"],
            google_sheet_info["spreadsheet_id"],
            google_sheet_info["output_sheet_name"]
        )
        
        # Assume the first row is headers
        records = input_sheet.get_all_records()

        # Initialize lists
        learning_objectives = []
        substandards = []
        key_phrases = []

        # Parse each record
        for idx, record in enumerate(records, start=1):
            try:
                learning_objective = record["Learning Objective"]
                substandard = json.loads(record["Substandards"])
                key_phrase = json.loads(record["Key Phrases"])

                learning_objectives.append(learning_objective)
                substandards.append(substandard)
                key_phrases.append(key_phrase)
            except json.JSONDecodeError as json_err:
                logging.error(f"JSON decoding failed for record {idx}: {str(json_err)}\nRecord Data: {record}")
                continue
            except KeyError as key_err:
                logging.error(f"Missing expected key in record {idx}: {str(key_err)}\nRecord Data: {record}")
                continue
    else:
        logging.error("No Google Sheet information provided in the event data.")
        learning_objectives = []
        substandards = []
        key_phrases = []

    if not learning_objectives:
        logging.warning("No learning objectives found to process.")
        return

    # Setup output file
    headers = ['Learning Objective', 'Substandards', 'Key Phrases', 'Thinking', 'Substandards to Key Phrases Mapping', 'Number of Key Phrases', 'Total Key Phrases Mapped', 'All Key Phrases Mapped Unique?']
    output_folder = "outputs"
    os.makedirs(output_folder, exist_ok=True)
    output_file = f"{event.get('output_file_name')}-mapping-output.csv"
    output_path = os.path.join(output_folder, output_file)
    logging.info(f"Writing mappings to: {output_path}")
    # Create file and write headers if not exists
    if not os.path.exists(output_path):
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
            logging.info(f"Created output CSV file with headers: {output_path}")
        except Exception as e:
            logging.error(f"Failed to create output CSV file: {str(e)}")
            sys.exit(1)

    # Setup multiprocessing
    with multiprocessing.Manager() as manager:
        lock = manager.Lock()
        pool = multiprocessing.Pool(processes=7, maxtasksperchild=5)
        
        # Process objectives and write results
        async_results = []
        for idx, (learning_objective, substandard, key_phrase) in enumerate(zip(
            learning_objectives, substandards, key_phrases
        ), start=1):
            if not learning_objective:
                logging.warning(f"Encountered empty Learning Objective at index {idx}. Skipping...")
                continue
            result = pool.apply_async(
                process_objective, 
                args=(
                    event, 
                    output_path, 
                    headers, 
                    lock, 
                    learning_objective, 
                    substandard, 
                    key_phrase
                )
            )
            async_results.append(result)
        
        # Wait for all processes to complete
        for result in async_results:
            try:
                result.get()  # This ensures we catch any exceptions in the child processes
            except Exception as e:
                logging.error(f"An error occurred during multiprocessing: {str(e)}")
        
        pool.close()
        pool.join()
    
    # After processing, write results to Google Sheet
    if google_sheet_info:
        try:
            logging.info("Reading mappings from CSV for Google Sheet update")
            with open(output_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
            
            logging.info("Appending results to the Output Sheet")
            for row in rows:
                output_sheet.append_row([
                    row['Learning Objective'],
                    row['Substandards'],
                    row['Key Phrases'],
                    row['Thinking'],
                    row['Substandards to Key Phrases Mapping'],
                    row['Number of Key Phrases'],
                    row["Total Key Phrases Mapped"],
                    row["All Key Phrases Mapped Unique?"]
                ])
            logging.info("Successfully wrote mappings to Google Sheet")
        except Exception as e:
            logging.error(f"Failed to write to Google Sheet: {str(e)}")
