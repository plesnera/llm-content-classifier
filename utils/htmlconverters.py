#! /usr/bin/env python3

from weasyprint import HTML
from bs4 import BeautifulSoup
import html2text
import uuid
from selenium import webdriver
import os
import base64


def html_to_pdf(html_object, output_target=None):
    """
    Render HTML to PDF using WeasyPrint.

    Args:
        html_file_path (str): Path to the input HTML file.
        output_pdf_path (str): Path to save the output PDF file.

    Returns:
        None
    """
    try:
        # Load the HTML file
        html = HTML(string=html_object)

        # Render HTML to PDF
        pdf = html.write_pdf()
        if output_target:
    # generate a string with a short random uuid for naming of file
            output_path = output_target + '/' + str(uuid.uuid4()) + '.pdf'
        # Save the PDF to the specified output path
            with open(output_path, 'wb') as pdf_file:
                pdf_file.write(pdf)
            print(f"PDF saved successfully at: {output_path}")
        return pdf

    except Exception as e:
        print(f"Error: {e}")


def html_to_text (html_content, output_target=None):
    """
    Extract text from an HTML document.

    Args:
        html_content (str): HTML document content as a string.

    Returns:
        str: Extracted text from the HTML document.
    """
    try:
        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        # Convert HTML to plain text using html2text
        text_content = html2text.html2text(str(soup))

        if output_target:
            output_target = output_target + '/' + str(uuid.uuid4()) + '.txt'
        # Take the raw text string and output to a txt file
            with open(output_target, 'w') as file:
                file.write(text_content)
                print(f"Text saved successfully at: {output_target}")

        return text_content
    except Exception as e:
        print(f"Error: {e}")
        return None


class FileSaver:
    def save(self, content, ending, target='tests'):
        filename = f"{target}/{uuid.uuid4()}{ending}"
        with open(filename, 'w') as file:
            file.write(content)
        return filename

class WebDriverInitializer:
    def initialize(self):
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        driver = webdriver.Chrome(options=options)
        return driver

class HtmlToPngConverter:
    def __init__(self, file_saver, web_driver_initializer):
        self.file_saver = file_saver
        self.web_driver_initializer = web_driver_initializer


    def prepare_content(self, html, output_target='/tmp'):
        if not os.path.isfile(html) and not isinstance(html,str):
            raise ValueError("Either html_content or file_path must be provided")
        if os.path.isfile(html):
            filename = os.path.abspath(html)
        else:
            filename = os.path.abspath(self.file_saver.save(html, ending='.html', target=output_target))
        return filename


    def render(self, filename, screen_width=1920):
        renders=[]
        driver = self.web_driver_initializer.initialize()
        driver.get(f"file://{filename}")

    # A blank object is created and Total height is used to prepare stitching of scrolling pages
        total_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")

    # Scroll through page and capture screenshots
        for i in range(0, total_height, viewport_height):
            driver.execute_script(f"window.scrollTo(0, {i});")

            png = driver.get_screenshot_as_png()
            renders.append(base64.b64encode(png).decode('utf-8'))
        driver.quit()
        return renders


if __name__ == "__main__":
    pass
