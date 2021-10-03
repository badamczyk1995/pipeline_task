# pipeline_task

Takes the government price paid dataset in csv format and utilises an Apache Beam pipeline to convert it to newline delimited json format with transactions grouped per their property. 

The data/sample_200k.csv file is a random sample (n=200,000) of transactions taken from the "the complete Price Paid Transaction Data as a CSV file (CSV, 4.3GB)" located here: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

The output/result_200k.ndjson file is the output from the pipeline on the sample_200k.csv.

How to use:
1. Modify the input_filename and ouput_filename variables inside pipeline_task.py
2. Run pipeline_task.py

