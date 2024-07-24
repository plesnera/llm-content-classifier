# README

This Python script is used to classify web content into appropriate IAB categories based on IAB taxonomy V2. It also classifies the content based on age and gender it would most likely appeal to.

## Dependencies
- openai
- dataclasses
- pyarrow
- google-cloud-storage
- pathlib
- csv

It also assumes an environment variable 'OPEN_AI_TOKEN' with a valid token.


## File Format
The `process_file` method in the `FileProcessor` class expects a Parquet file with at least two columns: 'body' and 'parsed_url'.

A small sample of the expected format has been included in this repo name data_sample.parquet 

Here is an example of the expected schema:

| parsed_url      | body |
|-----------------|------|
| www.example.com | `<html>...</html>` |
| www.test.com    | `<html>...</html>` |

## Usage

First, initialize the classifier, file processor, and directory traverser:

```python
classifier = Classifier(LLM)
file_processor = FileProcessor(classifier)
directory_traverser = DirectoryTraverser(file_processor)
```
To process a single file, use the process_file method:
```python
results = file_processor.process_file(
    file='/path/to/your/file.parquet',
    uri='file://',
    classification_type='text',
    test_mode_max_rows=5)
```
To traverse a directory and process all files within it, use the traverse_dir method:
```python
directory_traverser.traverse_dir(source_bucket_name='your-bucket-name', source_prefix='your-prefix')
```
Finally, write the results to a CSV file:
```python
CSVWriter.write_array_to_csv(results, 'results.csv')
```