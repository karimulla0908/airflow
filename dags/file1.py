import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import json
import re
from datetime import datetime
import inflect
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import os

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

p = inflect.engine()


current_datetime = datetime.now()
datetime_string = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")


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
            # Ensure both columns are numeric
            df[singular] = pd.to_numeric(df[singular], errors='coerce')
            df[plural] = pd.to_numeric(df[plural], errors='coerce')
            
            # Sum numeric values
            df[singular] = df[[singular, plural]].sum(axis=1, skipna=True)
            df = df.drop(columns=[plural])
    
    return df

def remove_numerical_columns(df):
    logging.info("Removing numerical columns.")
    # Identify columns that are numeric (integers and floats)
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
        match = re.match(r'(\w+)\s+\(Out of (\d+) Floors\)', floor)
        if match:
            current_floor = match.group(1)
            total_floors = int(match.group(2))
            
            # Convert textual representation of floors to numerical
            if current_floor.lower() == 'ground':
                current_floor = 0
            elif current_floor.lower() == 'top':
                current_floor = total_floors
            else:
                current_floor = int(current_floor)
    return current_floor, total_floors

# Define the categories and keywords for landmark classification
categories = {
    "Essentials Hub": ["school", "international school", "high school", "academy", "vidya","Vidyalayam","college","university","clg","Apartment"
                 "hospital", "clinic", "motherhood","church","mosque","Training"
                 "railway station", "metro station", "junction", "station","metro","bus","airport","Bus Stop",
                 "bank","Financial","Bank","temple"],
    "Urban Framework": ["property", "building", "block", "street","main road","road","bridge","layout","hotel","flyover","residence","park","Lake","Garden","road","pg","circle","Logistics","petrol","fire","nagar","toll","Centre","view point"],
    "Commercial Sphere": ["market", "mall", "shopping", "complex", "plaza", "store","bazaar","showroom",
                   "Tech Park","tech park","infotech","Software","IT","Resort","restaurant"]
}

def categorize_landmark(landmark):
    logging.info(f"Categorizing landmark: {landmark}")
    if not isinstance(landmark, str):
        return "Other"
    
    landmark_lower = landmark.lower()
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in landmark_lower:
                return category
    return "Other"

def process_flooring_column(flooring):
    logging.info("Processing the flooring types.")
    if isinstance(flooring, str):
        # Split by commas and strip whitespaces
        flooring_types = [f.strip() for f in flooring.split(',')]
        # Return the first flooring type or 'General' if empty
        return flooring_types[0] if flooring_types else 'other'
    else:
        return 'other'
    
def categorize_overlooking(value):
    if value == 'Not Disclosed':
        return 'Not Disclosed'
    elif ', ' in value:
        return 'Multiple Aspects'
    else:
        
        return 'Single Aspect'

def process_all_data():
    start_url = "https://www.magicbricks.com/flats-in-bangalore-for-sale-pppfs"
    max_pages = 30  # Adjust the number of pages to scrape

    logging.info(f"Starting data extraction from {start_url}.")

    property_urls = get_urls_from_multiple_pages(start_url, max_pages)

    # Save URLs to CSV file
    urls_df = pd.DataFrame(property_urls, columns=["url"])
    urls_file_name = f"property_urls_{datetime_string}.csv"
    urls_df.to_csv(urls_file_name, index=False)
    logging.info(f"Saved property URLs to {urls_file_name}.")

    # Extract data for each property URL
    all_property_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(extract_property_data, url): url for url in property_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                property_data = future.result()
                if property_data:
                    all_property_data.append(property_data)
            except Exception as e:
                logging.error(f"An error occurred while processing URL {url}: {e}")

    # Save raw property data to CSV file
    raw_property_data_df = pd.DataFrame(all_property_data)
    raw_property_data_file_name = f"raw_property_data_{datetime_string}.csv"
    raw_property_data_df.to_csv(raw_property_data_file_name, index=False)
    logging.info(f"Saved raw property data to {raw_property_data_file_name}.")

    # Clean and process the DataFrame
    property_data_df = raw_property_data_df.copy()

    logging.info("Starting data cleaning and processing.")

    # Remove numerical columns
    property_data_df = remove_numerical_columns(property_data_df)

    print(property_data_df.columns)

    # Convert price columns to lakh
    for column in property_data_df.columns:
        if 'price' in column.lower() or 'amount' in column.lower():
            property_data_df[column] = property_data_df[column].apply(convert_to_lakh)
            
    if 'Address' in property_data_df.columns:
        property_data_df['Address'] = property_data_df['Address'].apply(extract_bangalore_area)

    # Extract current floor and total floors
    property_data_df[['Current Floor', 'Total Floors']] = property_data_df['Floor'].apply(lambda x: pd.Series(process_floor_column(x)))

    property_data_df = property_data_df.drop(columns=['Floor'])

    # Merge similar columns
    property_data_df = merge_similar_columns(property_data_df)

    if 'Flooring' in property_data_df.columns:
        property_data_df['Processed Flooring'] = property_data_df['Flooring'].apply(process_flooring_column)
    
    if 'Lift' in property_data_df.columns:
        property_data_df['Lifts'] = property_data_df['Lift'].fillna(0)

    if 'covered-parking' in property_data_df.columns:
        # Replace numbers with 'Yes' and missing values with 'No'
        property_data_df['covered-parking'] = property_data_df['covered-parking'].apply(lambda x: 'Yes' if pd.notna(x) and x != 0 else 'No')

    if 'Age of Construction' in property_data_df.columns:
        def categorize_age(age):
            if isinstance(age, str) and age.lower() in ["new construction", "under construction"]:
                return age
            else:
                return "Old Construction"
        property_data_df['Construction Age'] = property_data_df['Age of Construction'].apply(categorize_age)
        property_data_df.drop(columns=["Age of Construction"],inplace=True)

    # Categorize landmarks
    if 'Landmarks' in property_data_df.columns:
        property_data_df['Landmark Category'] = property_data_df['Landmarks'].apply(categorize_landmark)

    columns_to_drop = ['Landmarks', 'Furnishing', 'Project', 'Developer', 'Flooring', 'Booking Amount', 'Super Built-up Area', 'Loan Offered','Lift',"Car parking"]

    if 'Overlooking' in property_data_df.columns:
        property_data_df['Overlooking']=property_data_df['Overlooking'].fillna("Not Disclosed")
        property_data_df.loc[:, 'Overlooking'] = property_data_df['Overlooking'].apply(categorize_overlooking)

    property_data_df = property_data_df.drop(columns=[col for col in columns_to_drop if col in property_data_df.columns])
    property_data_df = remove_columns_with_high_missing_values(property_data_df)
    # Save the final cleaned data to CSV file
    cleaned_property_data_file_name = f"cleaned_property_data_{datetime_string}.csv"
    property_data_df.to_csv(cleaned_property_data_file_name, index=False)
    upload_to_azure_datalake("cleaneddata0908",cleaned_property_data_file_name,"sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-20T22:30:39Z&st=2024-08-05T14:30:39Z&spr=https&sig=oZdqhWAKZzpArX7Y1pwkY5u3AR8SRu%2FJQvkUgyeQ9p0%3D")
    logging.info(f"Saved cleaned property data to {cleaned_property_data_file_name}.")
    print(property_data_df.columns)

run_task = PythonOperator(
    task_id='run_data_processing',
    python_callable=process_all_data,
    dag=dag,
)

run_task
