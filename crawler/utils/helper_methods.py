from bs4 import BeautifulSoup
import os
import requests

download_dir = ""


def get_text_from_html(html_content):
    soup = BeautifulSoup(html_content, features="html.parser")

    # kill all script and style elements.
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = ' '.join(chunk for chunk in chunks if chunk)
    return text


def get_pdf_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Find all <a> tags that have an 'href' attribute ending with '.pdf'
    pdf_links = soup.find_all('a', href=lambda x: x and x.endswith('.pdf'))
    return pdf_links


def download_pdf(pdf_link):
    downloaded_pdfs = []
    pdf_path = os.path.join(download_dir, os.path.basename(pdf_link))

    with requests.get(pdf_link, stream=True) as response:
        if response.status_code == 200:
            with open(pdf_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            downloaded_pdfs.append(pdf_path)

    return downloaded_pdfs
