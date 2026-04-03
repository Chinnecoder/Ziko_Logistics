# Import Necessary Libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

# Extraction Layer
ziko_df = pd.read_csv('ziko_logistics_data.csv')

# Data Cleaning and Transformation
ziko_df.fillna({
    'Unit_Price': ziko_df['Unit_Price'].mean(),
    'Total_Cost': ziko_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'Unknown'
}, inplace=True)

# Convert date columns to datetime format
ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])

# Customer Table
customer = ziko_df[[ 'Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)

# Product Table
product = ziko_df[['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Discount_Rate']].copy().drop_duplicates().reset_index(drop=True)

# Transaction Fact Table
transaction_fact = ziko_df.merge(customer, on=['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address'], how='left') \
                          .merge(product, on=['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Discount_Rate'], how='left') \
                          [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID', 'Total_Cost', 'Sales_Channel','Order_Priority',\
                            'Warehouse_Code', 'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason',\
                            'Payment_Type', 'Taxable', 'Region', 'Country']]

#Temporary Loading
customer.to_csv(r'dataset/customer.csv', index=False)
product.to_csv(r'dataset/product.csv', index=False)
transaction_fact.to_csv(r'dataset/transaction_fact.csv', index=False)

print("files have been temporarily loaded into local machine")


# Data Loading to Azure Blob Storage

# Azure Blob Connection
# Define your Azure Blob Storage connection string and container name
load_dotenv()  # Load environment variables from .env file

connection_str = os.getenv("CONNECTION_STR")
blob_service_client = BlobServiceClient.from_connection_string(connection_str)


container_name = os.getenv("CONTAINER_NAME")
container_client = blob_service_client.get_container_client(container_name)


# Create a function to load the DataFrame to Azure Blob Storage as a parquet file
def upload_df_to_bob_as_parquet(df, container_client, blob_name):

    # Convert DataFrame to parquet format in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)  # Move the buffer position to the beginning
    
    # Create a BlobClient and upload the parquet file
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_buffer, blob_type="BlockBlob", overwrite=True)
    print(f"DataFrame successfully uploaded to Azure Blob Storage as {blob_name}")

upload_df_to_bob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_bob_as_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_to_bob_as_parquet(transaction_fact, container_client, 'rawdata/transaction_fact.parquet')


