from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import json
import re
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30, 16, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'END-END-MLOPS',
    default_args=default_args,
    description='Scraping DAGS',
    schedule=None,
)

# Define Python functions for the tasks
def print_start():
    logging.info("Start of the DAG")

def print_processing():
    logging.info("Processing task")

def print_end():
    logging.info("End of the DAG")

current_datetime = datetime.now()
datetime_string = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

def merge_similar_columns(df):
    def get_column_pairs(columns):
        singular_plural_pairs = {}
        for col in columns:
            if col.endswith('s') and col[:-1] in columns:
                singular_plural_pairs[col[:-1]] = col
        return singular_plural_pairs

    columns = df.columns
    column_pairs = get_column_pairs(columns)
    
    for singular, plural in column_pairs.items():
        if singular in df.columns and plural in df.columns:
            df[singular] = df[[singular, plural]].sum(axis=1, skipna=True)
            df = df.drop(columns=[plural])
    
    return df

def remove_columns_with_high_missing_values(df):
    missing_percentage = df.isnull().mean() * 100
    columns_to_keep = missing_percentage[missing_percentage < 50].index
    df_cleaned = df[columns_to_keep]
    return df_cleaned

def convert_to_lakh(value):
    if isinstance(value, str):
        value = value.strip()
        if 'Lac' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return num
        elif 'Cr' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return num * 100
        elif 'Thousand' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return num / 100
        else:
            return np.nan
    return np.nan

# Azure Blob Storage details
account_name = "housescrape0908"
sas_token = "sv=2022-11-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2024-06-04T00:05:31Z&st=2024-05-30T16:05:31Z&spr=https&sig=9VaLndheHqoP1qqGPvKxrQsAbh9HlndJswAgbjyirzg%3D"

def extract_urls_from_scripts(html_content):
    logging.info("Extracting URLs from script tags.")
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        script_tags = soup.find_all("script", type="application/ld+json")
        urls = []
        for script in script_tags:
            json_data = script.string
            if json_data:
                try:
                    data = json.loads(json_data)
                    url = data.get("url")
                    if url:
                        urls.append(url)
                except json.JSONDecodeError as e:
                    logging.error(f"JSON decode error: {e}")
        logging.info(f"Found {len(urls)} URLs in script tags.")
        return urls
    except Exception as e:
        logging.error(f"An error occurred while extracting URLs: {e}")
        return []

def get_urls_from_multiple_pages(start_url, max_pages):
    logging.info(f"Starting to scrape URLs from {max_pages} pages starting at {start_url}.")
    all_urls = []
    for page_num in range(1, max_pages + 1):
        try:
            current_url = f"{start_url}?page={page_num}"
            logging.info(f"Making request to {current_url}.")
            response = requests.get(current_url, timeout=10)
            if response.status_code == 200:
                current_urls = extract_urls_from_scripts(response.content)
                all_urls.extend(current_urls)
                logging.info(f"Scraped {len(current_urls)} URLs from page {page_num}.")
            else:
                logging.error(f"Failed to retrieve page {page_num}. Status code: {response.status_code}")
                break
        except requests.RequestException as e:
            logging.error(f"An error occurred while making a request to page {page_num}: {e}")
            break
    logging.info(f"Finished scraping. Total URLs collected: {len(all_urls)}.")
    return all_urls

def upload_to_azure_blob(account_name, container_name, source_file_name, sas_token):
    try:
        # Construct the BlobServiceClient using the SAS token
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)
        
        # Specify the destination blob name as just the file name to upload it directly into the "urls" directory
        destination_blob_name = f"{source_file_name}"

        # Get the BlobClient for the destination blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=destination_blob_name)

        # Upload the file
        with open(source_file_name, "rb") as file:
            blob_client.upload_blob(file, overwrite=True)

        logging.info(f"File {source_file_name} uploaded to Azure Blob Storage as {destination_blob_name}.")
    except Exception as e:
        logging.error(f"An error occurred while uploading file to Azure Blob Storage: {e}")

def extract_property_data(url):
    logging.info(f"Extracting property data from {url}.")
    try:
        response = requests.get(url, timeout=10)
        html_content = response.text

        soup = BeautifulSoup(html_content, 'html.parser')

        data = {}

        # Extracting summary items
        summary_items = soup.select('.mb-ldp__dtls__body__summary--item')
        for item in summary_items:
            key = item['data-icon']
            highlight_span = item.find('span', class_='mb-ldp__dtls__body__summary--highlight')
            if highlight_span:
                value = highlight_span.text.strip()
                data[key] = value
            else:
                data[key] = None

        # Extracting list items
        list_items = soup.select('.mb-ldp__dtls__body__list--item')
        for item in list_items:
            key = item.find('div', class_='mb-ldp__dtls__body__list--label').text.strip()
            value = item.find('div', class_='mb-ldp__dtls__body__list--value').text.strip()
            data[key] = value

        # Extracting more details items
        more_details_items = soup.select('.mb-ldp__more-dtl__list--item')
        for item in more_details_items:
            key = item.find('div', class_='mb-ldp__more-dtl__list--label').text.strip()
            value = item.find('div', class_='mb-ldp__more-dtl__list--value').text.strip()
            data[key] = value

        # Extracting price breakup to get booking amount if available
        price_breakup_item = soup.find('div', string="Price Breakup")
        if price_breakup_item:
            price_breakup_item_parent = price_breakup_item.find_parent(class_='mb-ldp__more-dtl__list--item')
            if price_breakup_item_parent:
                booking_amount_text = price_breakup_item_parent.find('div', class_='mb-ldp__more-dtl__list--value').text.strip()
                if "Approx. Registration Charges" in booking_amount_text:
                    booking_amount = booking_amount_text.split('|')[1].strip().replace(' Approx. Registration Charges', '').replace('₹', '')
                    data['Booking Amount'] = booking_amount
                else:
                    data['Booking Amount'] = booking_amount_text
            else:
                data['Booking Amount'] = None
        else:
            data['Booking Amount'] = None

        # Extracting carpet area and price per sqft using regular expressions
        description = soup.get_text()
        carpet_area_match = re.search(r'(\d+)\s*sqft', description)
        price_per_sqft_match = re.search(r'₹([\d,]+)/sqft', description)

        if carpet_area_match:
            data['Carpet Area'] = carpet_area_match.group(1)
        else:
            data['Carpet Area'] = None

        if price_per_sqft_match:
            data['Price per sqft'] = price_per_sqft_match.group(1).replace(',', '')
        else:
            data['Price per sqft'] = None

        # Extracting located floor and number of floors
        floor_info_match = re.search(r'(\d+)\s*\(Out of\s*(\d+)\s*Floors\)', description, re.IGNORECASE)
        if floor_info_match:
            data['Located Floor'] = floor_info_match.group(1)
            data['Number of Floors'] = floor_info_match.group(2)
        else:
            data['Located Floor'] = None
            data['Number of Floors'] = None

        logging.info(f"Extracted data: {data}")
        return data
    except Exception as e:
        logging.error(f"An error occurred while extracting property data: {e}")
        return {}

def clean_data(**kwargs):
    try:
        ti = kwargs['ti']
        data_list = ti.xcom_pull(task_ids='generate_url_task', key='property_data')
        logging.info(f"Data received from XCom: {data_list}")

        if not data_list:
            logging.error("No data received from XCom.")
            return

        df = pd.DataFrame(data_list)
        df = df.loc[:, ~df.columns.str.isdigit()]
        df = merge_similar_columns(df)
        df_cleaned = remove_columns_with_high_missing_values(df)
        if 'Address' in df_cleaned.columns:
            df_cleaned.loc[:,'Address'] = df_cleaned['Address'].astype(str).str.split(",").str[-2]
        if 'Flooring' in df_cleaned.columns:
            df_cleaned.loc[:,'Flooring'] = df_cleaned['Flooring'].astype(str).str.split(",").str[0]
        columns_to_drop = ['Project', 'Landmarks', 'Floor', 'Developer', 'Project', 'Furnishing', 'Booking Amount']
        df_cleaned = df_cleaned.drop(columns=[col for col in columns_to_drop if col in df_cleaned.columns], errors='ignore')
        if 'Price Breakup' in df_cleaned.columns:
            df_cleaned.loc[:,'Price Breakup'] = df_cleaned['Price Breakup'].astype(str).str.split("|").str[0]
            df_cleaned.loc[:,'Price Breakup'] = df_cleaned['Price Breakup'].apply(convert_to_lakh)
            df_cleaned.rename(columns={'Price Breakup': 'Price Breakup(Lakh)'}, inplace=True)
        if 'Price per sqft' in df_cleaned.columns:
            df_cleaned.rename(columns={'Price per sqft': 'Price per sqft(Thousand)'}, inplace=True)
        property_data_csv_file = f"cleaned_property_data_{datetime_string}.csv"
        print(df_cleaned)
        df_cleaned.to_csv(property_data_csv_file, index=False)
        property_data_container_name = "cleaneddata"
        upload_to_azure_blob(account_name, property_data_container_name, property_data_csv_file, sas_token)
        logging.info("Property data scraping and upload completed.")
    except Exception as e:
        logging.error(f"An error occurred during the processing: {e}")

def generate_url(**kwargs):
    start_url = "https://www.magicbricks.com/flats-in-bangalore-for-sale-pppfs"
    max_pages = 10
    container_name = "urls"

    urls = get_urls_from_multiple_pages(start_url, max_pages)
    
    if urls:
        data_list = []
        for url in urls:
            data_list.append(extract_property_data(url))
        
        # Push property data to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='property_data', value=data_list)
        logging.info(f"Pushed {len(data_list)} entries to XCom.")
    else:
        logging.warning("No URLs found to scrape.")

# Define the Airflow tasks
start_task = PythonOperator(
    task_id='start',
    python_callable=print_start,
    dag=dag,
)

processing_task = PythonOperator(
    task_id='processing',
    python_callable=print_processing,
    dag=dag,
)

generate_url_task = PythonOperator(
    task_id='generate_url_task',
    python_callable=generate_url,
    dag=dag,
)

cleaning_data_task = PythonOperator(
    task_id='cleaning_data_task',
    python_callable=clean_data,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end',
    python_callable=print_end,
    dag=dag,
)

start_task >> generate_url_task >> processing_task >> cleaning_data_task >> end_task

# Required packages
# azure-identity==1.12.0
# azure-storage-blob==12.14.1