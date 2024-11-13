import logging
import time
from key_phrase_mapper import orchestrator


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

event = {
    "output_file_name": f"{int(time.time())}",
    "claude_api_key": "--your api-key--",
    "model": "claude-3-5-sonnet-20241022",
    "temperature": 0,
    "google_sheet": {
        "credentials_file": "--your service account credentials json file--",
        "spreadsheet_id": "--your spreadsheet id--",
        "input_sheet_name": "Inputs",
        "output_sheet_name": "Outputs"
    }
}


def main(event):
    logging.info("Starting main process")
    event_data = event
    orchestrator(event_data)

if __name__ == "__main__":
    start_time = time.time()
    logging.info("Script started")
    main(event)
    logging.info(f"Script finished in {time.time() - start_time} seconds")
