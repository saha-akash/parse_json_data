# parse_json_data

**Create a pipeline workflow for extracting data from the JSON files, transforming it into a dimensional
model and importing it into a database/cluster.**

Some descriptives on the dataset can be found at http://jmcauley.ucsd.edu/data/amazon/links.html

**Must Haves:**
• Proper exception handling/logging for bulletproof runs (3 times a day)
• Expressive, re-usable, clean code
• Include dimensional modeling of data.
• Contain product price as one of the fields in the dimensional model.
• Handle duplicates.
• Download the source data from the pipeline itself and have the ability to do the same at regular
intervals.
• Use some scheduling framework or workflow platform (preferably airflow).

For this example, product review data is being considered. 
