import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import ndjson
import time
import hashlib


# defining custom arguments
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./output/')


# Parses items from csv into a python dictionary
class ParseCSV(beam.DoFn):
    def process(self, element):
        (element_unique_identifier,
         price,
         date_of_transfer,
         postcode,
         property_type,
         old_new,
         duration,
         paon,
         saon,
         street,
         locality,
         town_city,
         district,
         county,
         ppd_category_type,
         record_status) = element.split('","')

        return [{
            'Transaction Unique Identifier': element_unique_identifier.replace('"', '').replace('{', '').replace('}', ''),
            'Price': int(price),
            'Date of Transfer': date_of_transfer,
            'Postcode': postcode,
            'Property Type': property_type,
            'Old / New': old_new,
            'Duration': duration,
            'PAON': paon,
            'SAON': saon,
            'Street': street,
            'Locality': locality,
            'Town / City': town_city,
            'District': district,
            'County': county,
            'PPD Category Type': ppd_category_type,
            'Record Status': record_status.replace('"', '')
        }]


# Takes location fields of the transaction and hashes them into a unique property id
# then adds it to the dictionary
class AssignUPID(beam.DoFn):
    def generate_upid(self, element):
        # Collect property location fields
        uid_fields = [element['Postcode'], element['PAON'], element['SAON'], element['Street'], element['Locality'],
                      element['Town / City'], element['District'], element['County']]

        # Join fields together, clean up and hash
        unique_property_string = ''.join(uid_fields)
        unique_property_string = ''.join(e for e in unique_property_string if e.isalnum()).lower()
        upid = int(hashlib.sha1(unique_property_string.encode('utf-8')).hexdigest(), 16)
        return upid

    def process(self, element):
        # Append the upid to the transaction
        upid = self.generate_upid(element)
        element['Unique Property ID'] = upid
        return [element]

# Extract the upid and transaction into tuple as key, value pair to allow
# for grouping all transactions to their property
class CollectUPID(beam.DoFn):
    def process(self, element):
        upid = element['Unique Property ID']
        del element['Unique Property ID']
        return [(upid, element)]


# Convert the python dictionary into a ndjson string suitable for writing
class ConvertToNDJSON(beam.DoFn):
    def process(self, element):
        key, val = element
        output_dict = {key: val}
        return [ndjson.dumps([output_dict])]


if __name__ == '__main__':

    # Modify these
    input_filename = "./data/sample_200K.csv"
    output_filename = "./output/result_200k.ndjson"

    options = PipelineOptions()

    start = time.time()
    with beam.Pipeline(options=options) as p:
        transactions = (p
                        | "Read CSV" >> beam.io.ReadFromText(input_filename)
                        | "Convert CSV to Python Dict" >> beam.ParDo(ParseCSV())
                        | "Assign transactions' property a unique id" >> beam.ParDo(AssignUPID()))

        property_transactions = (transactions
                                 | "Convert transaction into upid key, value pair" >> beam.ParDo(CollectUPID())
                                 | "Group transactions by their property" >> beam.GroupByKey())

        output = (property_transactions
                  | "Convert dictionary to ndjson" >> beam.ParDo(ConvertToNDJSON())
                  | "Write ndjson to file" >> beam.io.WriteToText(output_filename))
    end = time.time()
    print('Time Taken', end-start)





