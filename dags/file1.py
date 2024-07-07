import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import json
import re
from datetime import datetime
import spacy
import inflect
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import os

# Initialize the inflect engine
p = inflect.engine()

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")

# Setup logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

current_datetime = datetime.now()
datetime_string = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

def merge_similar_columns(df):
    logging.info("Merging similar columns (singular and plural forms).")
    def get_column_pairs(columns):
        singular_plural_pairs = {}
        for col in columns:
            singular_form = p.singular_noun(col) or col
            plural_form = p.plural_noun(col) or col
            if singular_form in columns and plural_form in columns:
                singular_plural_pairs[singular_form] = plural_form
        return singular_plural_pairs

    columns = df.columns
    column_pairs = get_column_pairs(columns)
    
    for singular, plural in column_pairs.items():
        if singular in df.columns and plural in df.columns:
            df[singular] = pd.to_numeric(df[singular], errors='coerce')
            df[plural] = pd.to_numeric(df[plural], errors='coerce')
            df[singular] = df[[singular, plural]].sum(axis=1, skipna=True)
            df = df.drop(columns=[plural])
    
    return df

def remove_numerical_columns(df):
    logging.info("Removing numerical columns.")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df = df.drop(columns=numeric_cols)
    return df

def remove_columns_with_high_missing_values(df):
    logging.info("Removing columns with high missing values (70% or more).")
    missing_percentage = df.isnull().mean() * 100
    columns_to_keep = missing_percentage[missing_percentage < 70].index
    df_cleaned = df[columns_to_keep]
    return df_cleaned

def convert_to_lakh(value):
    if isinstance(value, str):
        value = value.strip()
        if 'Lac' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return round(num, 2)
        elif 'Cr' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return round(num * 100, 2)
        elif 'Thousand' in value:
            num = float(re.findall(r"[\d\.]+", value)[0])
            return round(num / 100, 2)
        else:
            return np.nan
    return np.nan

def extract_urls_from_scripts(html_content):
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
        return urls
    except Exception as e:
        logging.error(f"An error occurred while extracting URLs: {e}")
        return []

@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def fetch_url(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response

def get_urls_from_multiple_pages(start_url, max_pages):
    logging.info(f"Starting URL extraction from {start_url} for {max_pages} pages.")
    all_urls = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(fetch_url, f"{start_url}?page={page_num}"): page_num for page_num in range(1, max_pages + 1)}
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                response = future.result()
                current_urls = extract_urls_from_scripts(response.content)
                all_urls.extend(current_urls)
                logging.info(f"Extracted URLs from page {page_num}.")
            except RetryError as e:
                logging.error(f"Failed to retrieve page {page_num} after retries: {e}")
            except Exception as e:
                logging.error(f"An error occurred while making a request to page {page_num}: {e}")
    return all_urls

@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def fetch_property_data(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response

def extract_property_data(url):
    logging.info(f"Extracting property data from {url}.")
    try:
        response = fetch_property_data(url)
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
        logging.info(f"Successfully extracted data from {url}.")
        return data
    except Exception as e:
        logging.error(f"An error occurred while extracting property data from {url}: {e}")
        return None

def extract_bangalore_area(address):
    if not isinstance(address, str):
        return "Bangalore"
    match = re.search(r"Bangalore\s*-\s*(\w+)", address)
    if match:
        return f"Bangalore - {match.group(1)}"
    else:
        return "Bangalore"

def process_floor_column(floor):
    logging.info("Processing the floors.")
    current_floor = None
    total_floors = None
    
    if isinstance(floor, str):
        match = re.match(r'(\w+)\s*of\s*(\d+)', floor)
        if match:
            current_floor_str = match.group(1)
            total_floors = int(match.group(2))
            
            if current_floor_str.lower() == 'ground':
                current_floor = 0
            elif current_floor_str.lower() == 'basement':
                current_floor = -1
            elif current_floor_str.lower() == 'lower basement':
                current_floor = -2
            else:
                current_floor = int(current_floor_str) if current_floor_str.isdigit() else None
    
    return current_floor, total_floors

def process_all_data():
    try:
        start_url = "https://www.magicbricks.com/flats-in-bangalore-for-sale-pppfs"
        max_pages = 20
        
        # Step 1: Extract property URLs
        property_urls = get_urls_from_multiple_pages(start_url, max_pages)
        property_urls_df = pd.DataFrame(property_urls, columns=["URL"])
        property_urls_filename = f"property_urls_{datetime_string}.csv"
        property_urls_df.to_csv(property_urls_filename, index=False)
        logging.info(f"Property URLs saved to {property_urls_filename}.")
        
        # Step 2: Extract raw property data
        raw_property_data = []
        for url in property_urls:
            data = extract_property_data(url)
            if data:
                raw_property_data.append(data)
        
        raw_property_data_df = pd.DataFrame(raw_property_data)
        raw_property_data_filename = f"raw_property_data_{datetime_string}.csv"
        raw_property_data_df.to_csv(raw_property_data_filename, index=False)
        logging.info(f"Raw property data saved to {raw_property_data_filename}.")
        
        # Step 3: Clean the data
        cleaned_data_df = raw_property_data_df.copy()
        
        # Data Cleaning Steps
        cleaned_data_df = remove_columns_with_high_missing_values(cleaned_data_df)
        cleaned_data_df = merge_similar_columns(cleaned_data_df)
        cleaned_data_df = remove_numerical_columns(cleaned_data_df)
        cleaned_data_df['Price'] = cleaned_data_df['Price'].apply(convert_to_lakh)
        cleaned_data_df['Address'] = cleaned_data_df['Address'].apply(extract_bangalore_area)
        cleaned_data_df[['Current Floor', 'Total Floors']] = cleaned_data_df['Floor'].apply(lambda x: pd.Series(process_floor_column(x)))
        
        cleaned_property_data_filename = f"cleaned_property_data_{datetime_string}.csv"
        cleaned_data_df.to_csv(cleaned_property_data_filename, index=False)
        logging.info(f"Cleaned property data saved to {cleaned_property_data_filename}.")
        
    except Exception as e:
        logging.error(f"An error occurred during the data processing: {e}")

def upload_to_azure_datalake(container_name, blob_name, sas_token):
    try:
        blob_service_client = BlobServiceClient(account_url=f"https://cleaneddata0908.blob.core.windows.net", credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)

        local_file_path = blob_name
        blob_client = container_client.get_blob_client(blob=blob_name)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Successfully uploaded {blob_name} to Azure Data Lake.")
    except Exception as e:
        logging.error(f"An error occurred while uploading to Azure Data Lake: {e}")

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_processing_and_upload',
    default_args=default_args,
    description='Process data and upload to Azure Data Lake',
    schedule_interval=timedelta(days=1),
)

def run_data_processing():
    process_all_data()
    
    sas_token = "your_sas_token"
    container_name = 'your_container_name'
    
    file_names = [
        f"property_urls_{datetime_string}.csv",
        f"raw_property_data_{datetime_string}.csv",
        f"cleaned_property_data_{datetime_string}.csv"
    ]
    
    for file_name in file_names:
        upload_to_azure_datalake(container_name, file_name, sas_token)

run_task = PythonOperator(
    task_id='run_data_processing',
    python_callable=run_data_processing,
    dag=dag,
)
