import os
from openai import OpenAI
from dataclasses import dataclass
from utils import htmlconverters
import pyarrow.parquet as pq
import csv
from google.cloud import storage
import pathlib as pl


# TODO Define types for all functions
# TODO Add pydantic and implement LLM response type checks
# TODO Add retry when response is missing attribute(s)
# TODO Add check for valid IAD category
# TODO add check for correcly formatted response

@dataclass
class LLM:
    client = OpenAI(api_key=os.environ['OPEN_AI_TOKEN'])
    model = 'gpt-4o'
    role = 'user'


class Classifier:
    def __init__(self, classifier_model):
        self.client = classifier_model.client
        self.role = classifier_model.role
        self.model = classifier_model.model

    def _generate_prompt(self, url):
        age_groups = ['1-15 years', '16-20 years', '21-25 years', '26-30 years', '31-40 years', '41-50 years', 'older']
        genders = ['male', 'female', 'both']
        classification_prompt = (
                "Given a url and the content of a webpage, please classify it into the appropriate IAB categories based "+
                "on IAB taxonomy V2. Also return a classification of what age and gender it would most "+
                "likely appeal to. The gender should if possible be one of the following: " + ",".join(genders) + ". If unable"+
                "to classify, please respond with 'unknown'. The age should be within one of the following groups '" +
                "','".join(age_groups) + "' or if unable to classify, please respond with 'unknown'. Please provide a json formatted response" +
                "with the following keys 'age group', 'gender', 'IAB category'" +
                f"Url to classify: '{url}'"
        )
        return classification_prompt

    def parse_response(self, response):
        try:
            age_group = response.choices[0].message.content.split(',')[0]
            gender = response.choices[0].message.content.split(',')[1]
            iab_category = response.choices[0].message.content.split(',')[2]

        except:
            age_group = 'unknown'
            gender = 'unknown'
            iab_category = 'unknown'
        return age_group, gender, iab_category

    def classify_text(self, url, content):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": self.role,
                    "content": self._generate_prompt(url)
                },
                {
                    "type": "text",
                    "text": content
                }]
        )
        return self.parse_response(response)

    def produce_classification_message(self, url, content_array ):
        message=[]
        message.append({"role": self.role, "content":[]})
        message[0]["content"].append(
                {
                    "type":"text",
                    "text": self._generate_prompt(url)
                }
        )

        for encoded_image in content_array:
            message[0]["content"].append(
                {
                    "type":"image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{encoded_image}"
                    }
                }
            )
        return message


    def classify_image(self, url, content):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.produce_classification_message(url, content)
        )
        return self.parse_response(response)

class FileProcessor:
    def __init__(self, classifier, image_processor):
        self.classifier = classifier
        self.image_processor=image_processor

    def process_file(self, file, uri=None, classification_type='text', test_mode_max_rows=None):
        """
        Process a Parquet file and classify its contents.

        This function reads a Parquet file, extracts 'body' and 'parsed_url' columns,
        and processes each row to determine gender, age group, and IAB category.

        Args:
            file (str or file-like object): The Parquet file to process.
            uri (str, optional): The URI of the filesystem if the file is not local.
            classification_type (str, optional): The type of classification to perform. Defaults to 'text' but can also be 'image'
            test_mode_max_rows (int, optional): Maximum number of rows to process in test mode. If None, process all rows.

        Returns:
            list: A list of tuples, each containing (url, classification_type, gender, age_group, iab_category) for each processed row.

        Note:
            This method requires the 'pyarrow' library for reading Parquet files.
            It also assumes the existence of a 'process_body' method in the class.
        """

        results = []
        # extract columns from parquet file
        table = pq.read_table(file, filesystem=uri)
        body_array = table.column('body')
        url_array = table.column('parsed_url')

        # Set Max rows to process in test mode
        if test_mode_max_rows != None:
            max_rows = test_mode_max_rows
        else:
            max_rows = len(body_array)
        body_array = body_array[:test_mode_max_rows]
        url_array = url_array[:test_mode_max_rows]
        print(f'Processing {max_rows} rows from {file}')

        # Iterate through the rows in the parquet file
        for i in range(max_rows):
            url = str(url_array[i])
            body = str(body_array[i])
            gender, age_group, iab_category = self.process_body(body,url, classification_type)
            results.append((url, classification_type, gender, age_group, iab_category))
        return results

    def process_body(self, body, url, classification_type='text'):
        gender, age_group, iab_category = None, None, None
        if classification_type == 'text':
            raw_text = htmlconverters.html_to_text(body)
            if raw_text != None:
                age_group, gender, iab_category = classifier.classify_text(url, raw_text)
        elif classification_type == 'image':
            tmp_file=self.image_processor.prepare_content(body)
            base64_image_array = self.image_processor.render(tmp_file,'renders/')
            if base64_image_array is not None:
                age_group, gender, iab_category = classifier.classify_image(url, base64_image_array)

        print(url, classification_type, age_group, gender, iab_category)
        return age_group, gender, iab_category


class DirectoryTraverser:
    def __init__(self, file_processor):
        self.file_processor = file_processor

    def traverse_dir(self, source_bucket_name, source_prefix=None):
        client = storage.Client()
        # Get the google cloud storage bucket
        bucket = client.get_bucket(source_bucket_name)
        # Get the list of files in the google cloud storage bucket
        blobs = bucket.list_blobs(prefix=source_prefix)
        # Iterate through the list of files
        for blob in blobs:
            file = pl.Path(bucket.name, blob.name)
            # Check if the file extension is parquet
            if file.suffix == '.parquet':
                # Split the parquet file
                file_processor.process_file(file)
                # Print the file name
                print(f'{file.name} has been processed')
            else:
                # Print the file name
                print(f'{file.name} is not a csv or parquet file')


class CSVWriter:
    @staticmethod
    def write_array_to_csv(array, file_path):
        try:
            with open(file_path, 'a') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(('url', 'age_group', 'gender'))
                for tuple_item in array:
                    writer.writerow(tuple_item)
            print(f"Array has been written to {file_path}")
        except Exception as e:
            print(f"Error: {e}")


if __name__ == '__main__':
    classifier = Classifier(LLM)
    file_saver = htmlconverters.FileSaver()
    web_driver_initializer = htmlconverters.WebDriverInitializer()
    image_converter = htmlconverters.HtmlToPngConverter(file_saver, web_driver_initializer)
    file_processor = FileProcessor(classifier, image_processor=image_converter)
    directory_traverser = DirectoryTraverser(file_processor)
    results = file_processor.process_file(
        file='data_sample.parquet',
        classification_type='image',
        test_mode_max_rows=2)
    # CSVWriter.write_array_to_csv(results, 'tests/file.txt')
