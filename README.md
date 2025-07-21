# Excel S3 Integration with Supabase PostgreSQL + Streamlit Dashboard

This project implements an AWS Lambda function that reads Excel files stored in an S3 bucket, processes and normalizes the data, and inserts or updates records in a PostgreSQL database table hosted on Supabase. Additionally, there is a Streamlit app for data visualization and analysis.

---

## Technologies Used

- **AWS Lambda**: Serverless function for automatic processing of Excel files.  
- **Amazon S3**: Storage for the Excel files.  
- **Supabase (PostgreSQL)**: Cloud relational database to store the data.  
- **Python**: Main language for processing and database connection.  
- **pandas**: For Excel data manipulation.  
- **psycopg2-binary**: PostgreSQL driver for database connection.  
- **SQLAlchemy**: ORM to facilitate SQL operations.  
- **Streamlit**: Web dashboard for data visualization.  
- **Docker + Lambda Layer**: To package Python dependencies compatible with AWS Lambda.

---

## Features

- List Excel files in the S3 bucket.  
- Read and concatenate data from Excel files.  
- Clean and transform data (drop columns, rename).  
- Insert or update data in the PostgreSQL table (`tb_vendas_mes`).  
- Visualize updated data via a Streamlit app connected to the Supabase database.
