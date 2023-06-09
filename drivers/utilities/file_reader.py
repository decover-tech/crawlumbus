import re
import urllib
import docx
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
import os
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from utilities.s3_client import S3Client


def read_pdf_with_ocr(file_path):
    """
    This method reads the contents of a PDF using OCR.
    :param file_path: The path to the PDF file.
    :return: The contents of the file.
    """
    print(f"Reading PDF with OCR: {file_path}")
    # Step I: Convert PDF to JPG.
    pdf_pages = convert_from_path(file_path, 500)

    # Step II: Create tempdir if it does not exist
    tempdir = os.path.join(os.path.dirname(file_path), 'temp')
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)

    # Step III: List of image files.
    image_file_list = []
    result = ''

    # Step IV: Convert PDF to JPG
    for page_enumeration, page in enumerate(pdf_pages, start=1):
        # Create a file name to store the image
        filename = f"{tempdir}/page_{page_enumeration:03}.jpg"

        # Declaring filename for each page of PDF as JPG
        # For each page, filename will be:
        # PDF page 1 -> page_001.jpg
        # PDF page 2 -> page_002.jpg
        # PDF page 3 -> page_003.jpg
        # PDF page n -> page_00n.jpg
        # ....

        # Save the image of the page in system
        page.save(filename, "JPEG")
        image_file_list.append(filename)
        text = str((pytesseract.image_to_string(Image.open(filename))))

        # Replace \n with space
        text = text.replace('-\n', '')
        result += text

        # Delete the temp file.
        if os.path.exists(filename):
            os.remove(filename)

    # Step V: Delete the temp directory.
    if os.path.exists(tempdir):
        os.rmdir(tempdir)

    # Step VI: Return the result
    return result

def read_local_file(file_path):
    """
    This method reads the contents of a file from the local file system.
    :param file_path:
    :return: The contents of the file.
    """
    if file_path.endswith('.pdf'): # Check if the input_file_name is a PDF
        with open(os.path.join(file_path), 'rb') as f: # Open the PDF and extract its contents
            resource_manager = PDFResourceManager()
            string_io = StringIO()
            converter = TextConverter(resource_manager, string_io)
            page_interpreter = PDFPageInterpreter(resource_manager, converter)
            for page in PDFPage.get_pages(f):
                page_interpreter.process_page(page)
        file_contents = string_io.getvalue()
        converter.close()
        string_io.close()
        contents_normalized = re.sub('[^a-zA-Z0-9. ]+', '', file_contents)
        if len(contents_normalized) > 0:
            return contents_normalized
        else:
            file_contents = read_pdf_with_ocr(file_path)
        return file_contents

    if file_path.endswith('.docx'): # Check if the input_file_name is a PDF
        doc = docx.Document(file_path)
        fullText = []
        for para in doc.paragraphs:
            fullText.append(para.text)
        return '\n'.join(fullText)
    else:
        # If the input_file_name is not a PDF, it is assumed to be a regular text input_file_name
        # Open the input_file_name and read its contents
        with open(os.path.join(file_path), 'r') as f:
            file_contents = f.read()
    return file_contents

def read_local_file_with_ocr(file_path):
    """
    This method reads the contents of a file from the local file system using OCR.
    :param file_path:
    :return: The contents of the file.
    """
    with open(os.path.join(file_path), 'rb') as f: # Open the PDF and extract its contents
        resource_manager = PDFResourceManager()
        string_io = StringIO()
        converter = TextConverter(resource_manager, string_io)
        page_interpreter = PDFPageInterpreter(resource_manager, converter)
        for page in PDFPage.get_pages(f):
            page_interpreter.process_page(page)
    contents = string_io.getvalue()
    converter.close()
    string_io.close()
    return contents

def read_file_from_S3(s3_client, file_path):
    """
    This method reads the contents of a file from S3.
    :param s3_client:
    :param file_path:
    :return:
    """
    # Get the bucket name and file name.
    file_path_decoded = urllib.parse.unquote(file_path)
    file_name = file_path_decoded.split('/')[3]
    # Read the file from S3.
    tmp_file = s3_client.get_file(file_name)
    contents = read_local_file(tmp_file)
    # Delete the temp file.
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
    return contents


class FileReader:
    """
    This class is responsible for reading the contents of a file.
    Supports the following:
    1. Reading a text file from a URL. (TODO)
    2. Reading a PDF file using OCR. (Done)
    3. Reading a text file from S3. (Done)
    4. Reading a text file from the local file system. (DONE)
    5. Reading a docx file from the local file system. (DONE)
    """
    def __init__(self):
        # The S3 client is used to read files from S3.
        self.s3_client = S3Client()
        self.contents = ''

    # TODO: Should support reading from a URL.
    def read(self, file_path: str) -> str:
        """
        This method reads the contents of a file and returns it.
        :param file_path: The path of the file.
        :return: The contents of the file.
        """
        # Step I: Check if the file is on S3.
        # What happens in the case when the file is on a website? We should try with request as well.
        if file_path.startswith('s3://') or file_path.startswith('http') or file_path.startswith('https'):
            self.contents = read_file_from_S3(self.s3_client, file_path)
        else:
            self.contents = read_local_file(file_path)
        return self.contents