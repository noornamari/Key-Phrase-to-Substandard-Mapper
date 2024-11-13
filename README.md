# Key Phrases to Substandards Mapping Tool
### Disclaimer: This tool uses an Anthropic prompt. 

## Steps:
## 1. Make a copy of this [Key Phrase Mapping Template sheet](https://docs.google.com/spreadsheets/d/1eKBtM7s35TVFJ5bbl6TlhSuH4oyAr9Vg6nHmzBV3SUs/edit?gid=0#gid=0). 
This is where you'll add the key phrases and substandards associated with the L1.

## 2. [Set up a google cloud service account.](https://cloud.google.com/iam/docs/service-accounts-create#creating) 
Note the email that is created (the google sheet needs to give edit access to this email). Then, get the service account key in JSON by going to "Manage Keys" and "Create a new key." The JSON file that has the key needs to be added to the folder that has the run.py script. 

## 3. Update the event dictionary in run.py.
Update the event with your claude API key, the path to your service account credentials, and your spreadsheet id. 

## 4. Add your learning objective, substandards, and key phrases inputs to your spreadsheet and execute run.py
The outputs will be pushed to the google sheet's "Outputs" tab. The outputs include the model's thinking process for the mapping, the dictionary of the mappings, the number of key phrases associated with the learning objective, the number of key phrases mapped, and if all of the key phrases that are mapped are only used once (i.e., are they unique?).

## 5. Execute the google AppsScript function "Format Mappings." 
This will output each substandard to a row in the sheet "Key Phrases Mapped to Substandards, "with each of the substandard's mapped key phrases in their own column.

Notes: 
- The "Format Mappings" button is under "Custom Tools" in the google sheet's Ribbon.
- Make sure to authorize google AppsScript when it asks you to. 
- Make sure you are on the "Outputs" sheet. 
