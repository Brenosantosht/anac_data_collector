import requests
from bs4 import BeautifulSoup
import boto3
import io


def lambda_handler(event, context):
    base_url = f"https://sistemas.anac.gov.br/dadosabertos/Operador Aeroportuário/Dados de Movimentação Aeroportuárias/"
    boto_client = boto3.client('s3')

    main_page = requests.get(base_url)
    soup = BeautifulSoup(main_page.content, 'html5lib')

    directories = soup.findAll('a', attrs={'href': True})
    directories.pop(0)

    for directory in directories:
        page = requests.get(base_url + directory['href'])
        page_content = BeautifulSoup(page.content, 'html5lib')
        files_links = page_content.findAll('a', attrs={'href': True})
        files_links.pop(0)

        for file in files_links:
            filename = f"./{file['href']}"
            if filename.split('.')[-1] != "csv":
                continue

            url = (base_url + directory['href'] + file['href'])
            download_file = requests.get(url).content
            boto_client.upload_fileobj(io.BytesIO(download_file), 'cloudstudiess3buck', file['href'])
