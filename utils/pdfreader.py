import PyPDF2
from tika import parser
import sys

sys.path.append("..")


def create_pdf_reader_object(pdfFileObj):
    return PyPDF2.PdfFileReader(pdfFileObj)


def get_text_from_pdf_using_pypdf(filename):
    pdfFileObj = open(filename, 'rb')
    pdfReader = create_pdf_reader_object(pdfFileObj)
    num_pages = pdfReader.numPages
    text = []
    for page_no in range(num_pages):
        page = pdfReader.getPage(page_no)
        print(page.extractText())

        text.extend(page.extractText().split())
    return " ".join(text)

def get_text_from_pdf_using_tika(filename):
    file_data = parser.from_file(filename)
    text = file_data['content']
    return text




