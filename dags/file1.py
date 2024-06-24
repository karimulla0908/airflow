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
import inflect

# Initialize the inflect engine
p = inflect.engine()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15, 13, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'END-END-MLOPS',
    default_args=default_args,
    description='Scraping DAG 0908',
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
            singular_form = p.singular_noun(col) or col  # Get the singular form or keep the column name if it's already singular
            plural_form = p.plural_noun(col) or col     # Get the plural form or keep the column name if it's already plural
            if singular_form in columns and plural_form in columns:
                singular_plural_pairs[singular_form] = plural_form
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
account_name = "housescrape09"
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-10-09T20:00:13Z&st=2024-06-24T12:00:13Z&spr=https&sig=0mQARoqDmL0vN5SAeUPVEyhR4ipSixeDpuo6VBvnqqU%3D"

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
        price_per_sqft_match = re.search(r'(\d+)\s*per sqft', description)

        if carpet_area_match:
            data['Carpet Area'] = carpet_area_match.group(1)
        if price_per_sqft_match:
            data['Price per sqft'] = price_per_sqft_match.group(1)

        return data
    except Exception as e:
        logging.error(f"An error occurred while extracting property data from {url}: {e}")
        return None

def process_all_data():
    # Task 1: Get all property URLs
    start_url = "https://www.magicbricks.com/flats-in-pune-for-sale-pppfs"
    max_pages = 3  # Adjust the number of pages to scrape

    logging.info(f"Starting URL scraping for {max_pages} pages from {start_url}.")
    property_urls = get_urls_from_multiple_pages(start_url, max_pages)

    # Save URLs to CSV file
    urls_df = pd.DataFrame(property_urls, columns=["url"])
    urls_file_name = f"property_urls_{datetime_string}.csv"
    urls_df.to_csv(urls_file_name, index=False)
    logging.info(f"Saved property URLs to {urls_file_name}.")

    # Upload URLs CSV to Azure Blob Storage
    upload_to_azure_blob(account_name, "scrapedfiles", urls_file_name, sas_token)

    # Task 2: Extract data for each property URL
    all_property_data = []
    for url in property_urls:
        property_data = extract_property_data(url)
        if property_data:
            all_property_data.append(property_data)
    logging.info(f"Extracted data for {len(all_property_data)} properties.")

    # Save property data to CSV file
    property_data_df = pd.DataFrame(all_property_data)
    property_data_file_name = f"property_data_{datetime_string}.csv"
    property_data_df.to_csv(property_data_file_name, index=False)
    logging.info(f"Saved property data to {property_data_file_name}.")

    # Task 3: Data cleaning and preprocessing
    property_data_cleaned = property_data_df.copy()
    property_data_cleaned = merge_similar_columns(property_data_cleaned)
    property_data_cleaned = remove_columns_with_high_missing_values(property_data_cleaned)

    # Convert price-related columns to numerical format in lakhs
    for col in property_data_cleaned.columns:
        if 'price' in col.lower() or 'amount' in col.lower() or 'cost' in col.lower():
            property_data_cleaned[col] = property_data_cleaned[col].apply(convert_to_lakh)

    # Save cleaned data to CSV file
    property_data_cleaned_file_name = f"property_data_cleaned_{datetime_string}.csv"
    property_data_cleaned.to_csv(property_data_cleaned_file_name, index=False)
    logging.info(f"Saved cleaned property data to {property_data_cleaned_file_name}.")

    # Upload cleaned data CSV to Azure Blob Storage
    upload_to_azure_blob(account_name, "cleaneddata", property_data_cleaned_file_name, sas_token)

# Define the tasks using PythonOperator
start_task = PythonOperator(
    task_id='start',
    python_callable=print_start,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process',
    python_callable=process_all_data,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end',
    python_callable=print_end,
    dag=dag,
)

# Set task dependencies
start_task >> process_task >> end_task
