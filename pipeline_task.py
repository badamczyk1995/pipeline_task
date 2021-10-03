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


class AssignUPID(beam.DoFn):
    def generate_upid(self, element):
        uid_fields = [element['Postcode'], element['PAON'], element['SAON'], element['Street'], element['Locality'],
                      element['Town / City'], element['District'], element['County']]

        unique_property_string = ''.join(uid_fields)
        unique_property_string = ''.join(e for e in unique_property_string if e.isalnum()).lower()
        upid = int(hashlib.sha1(unique_property_string.encode('utf-8')).hexdigest(), 16)
        return upid

    def process(self, element):
        upid = self.generate_upid(element)
        element['Unique Property ID'] = upid
        return [element]


class CollectUPID(beam.DoFn):
    def process(self, element):
        upid = element['Unique Property ID']
        del element['Unique Property ID']
        return [(upid, element)]


class ConvertToNDJSON(beam.DoFn):
    def process(self, element):
        key, val = element
        output_dict = {key: val}
        return [ndjson.dumps([output_dict])]


if __name__ == '__main__':
    input_filename = "./data/sample_200K.csv"
    output_filename = "./output/result_200k.ndjson"

    options = PipelineOptions()

    start = time.time()
    with beam.Pipeline(options=options) as p:
        transactions = (p
                        | beam.io.ReadFromText(input_filename)
                        | beam.ParDo(ParseCSV())
                        | beam.ParDo(AssignUPID()))

        property_transactions = (transactions
                                 | beam.ParDo(CollectUPID())
                                 | beam.GroupByKey())

        output = (property_transactions
                  | beam.ParDo(ConvertToNDJSON())
                  | beam.io.WriteToText(output_filename))
    end = time.time()
    print('Time Taken', end-start)





