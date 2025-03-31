# S3 to Snowflake ETL with Airflow

## 📌 Project Overview
This project automates the ETL process using **Apache Airflow** to load CSV files from an **AWS S3 bucket** into **Snowflake**. The workflow leverages Snowflake's **COPY INTO** command via `CopyFromExternalStageToSnowflakeOperator`.

## 📂 Repository Structure
```
bfs-etl-2025/
│── dags/
│   ├── s3_snowflakes.py       # Airflow DAG for data transfer
│   ├── test_connection.py     # DAG for testing Snowflake connection
│── project_1/
│   ├── create_table.py        # Script to generate dummy data
│   ├── *.csv                  # Dummy CSV files
│   ├── README.md              # Project Documentation
```

## ⚙️ Prerequisites
### 1️⃣ **Snowflake Stage** (Brian's Stage)
Since access to the S3 bucket is restricted, this project uses **Brian's stage** instead of creating a new one. The stage is pre-configured in Snowflake and accessible for querying.

### 2️⃣ **AWS S3 Bucket (Source Data)**
Ensure the CSV files exist in the S3 bucket:
- **Bucket Name:** `febde2025`
- **Folder Path:** `airflow_project/`
- **Example Files:**
  ```
  s3://febde2025/airflow_project/sale_data_team2_2025-03-12.csv
  s3://febde2025/airflow_project/sale_data_team2_2025-03-13.csv
  s3://febde2025/airflow_project/sale_data_team2_2025-03-14.csv
  ```

### 3️⃣ **Snowflake Table (Target)**
Ensure the Snowflake table exists:
```sql
CREATE OR REPLACE TABLE prestage_sale_data_team2 (
    id            NUMBER(38,0),
    name          VARCHAR(100),
    product_name  VARCHAR(200),
    category      VARCHAR(100),
    price         NUMBER(10,2),
    quantity      NUMBER(38,0),
    date          TIMESTAMP_NTZ,
    region        VARCHAR(100),
    status        VARCHAR(50),
    discount      VARCHAR(10),  
    total         NUMBER(10,2)
);
```

## 🚀 Workflow
### 1️⃣ **Airflow DAG Execution**
1. The DAG [`s3_snowflakes.py`](../dags/S3_Snowflakes.py) is triggered.
  - The DAG is scheduled to run daily (@daily), meaning it automatically triggers every day at midnight UTC starting from Mar 12, 2025.
  - Airflow loads configurations and connects to Snowflake using snowflake_conn_id.
  - Retries are enabled (retries=1, retry_delay=5 minutes), meaning if a failure happens, Airflow will retry once.
2. The DAG **directly loads CSV files from S3 to Snowflake** using `CopyFromExternalStageToSnowflakeOperator`.
3. The files are copied into the `PRESTAGE_SALE_DATA_TEAM2` table in Snowflake.

### 2️⃣ **Airflow Connection Setup**
Before running the DAG, the following **Airflow connections** must be set up:
#### **Snowflake Connection**
1. Log in to **Airflow UI** (`http://52.206.224.184:8080/login/`).
2. Go to **Admin > Connections**.
3. Click **"Add Connection"**.
4. Set the following:
   - **Connection ID**: `snowflake_conn`
   - **Connection Type**: `Snowflake`
   - **Account**: `<Snowflake Account>`
   - **Login**: `<Your Snowflake Username>`
   - **Password**: `<Your Snowflake Password>`
   - **Warehouse**: `BF_ETL0210`
   - **Database**: `AIRFLOW0210`
   - **Schema**: `BF_DEV`
   - **Role**: `BF_DEVELOPER0210`
   - **Extra (JSON format):**
     ```json
        {
            "warehouse": "BF_ETL0210",
            "database": "AIRFLOW0210",
            "role": "BF_DEVELOPER0210",
            "insecure_mode": false,
            "account": "<Snowflake Account>",
            "region": "us-east-1"
        }
     ```
5. Click **"Save"**.

## 🛠 Development Workflow
### **Step 1: Setup Your Environment**
- Create the **Snowflake connection** in Airflow.
- Confirm the **target location in Snowflake** (table and schema).

### **Step 2: Create and Upload Mock Data to S3**
- To simulate real-world data, a script (`create_table.py`) is used to generate mock sales data. The [script](./create_table.py) uses **Faker** and **random sampling** to create diverse product sales records.
- Upload all csv file into S3 bucket.

### **Step 3: Create the Snowflake Prestage Table**
- Manually create the target table in **Snowflake UI**.
- Use **Brian's stage (`S3_STAGE_TRANS_ORDER`)** as the external stage for data transfer.

### **Step 4: Develop an Airflow DAG**
- Implement `s3_snowflakes.py` to **load data from S3 to Snowflake**.
- Use `CopyFromExternalStageToSnowflakeOperator` to handle the transfer.
- Push to Github on branch team2 (`https://github.com/beaconfireDE/bfs-etl-feb2025/tree/team2`)
- Mannully trigger the DAG

### **Step 5: Validate the Data in Snowflake**
- Run the DAG and monitor logs in Airflow.
```
[2025-03-12, 19:41:19 EDT] {copy_into_snowflake.py:141} INFO - COPY command completed
[2025-03-12, 19:41:20 EDT] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=s3_to_snowflake_etl, task_id=S3_load_to_snowflakes, execution_date=20250312T234115, start_date=20250312T234118, end_date=20250312T234120
[2025-03-12, 19:41:20 EDT] {local_task_job_runner.py:234} INFO - Task exited with return code 0
[2025-03-12, 19:41:20 EDT] {taskinstance.py:3281} INFO - 0 downstream tasks scheduled from follow-on schedule check
```
- Confirm that the data is correctly loaded into Snowflake using:
  ```sql
  SELECT count(*) FROM PRESTAGE_SALE_DATA_TEAM2;
  ```
## 🛠 Troubleshooting
### ❌ **Column Mismatch Error**
**Error:**
```
Number of columns in file (11) does not match that of the corresponding table (12)
```
✅ **Solution:**
- Ensure the **CSV files have the correct column structure**.
- OR Modify the DAG to allow mismatched columns:
  ```python
  file_format="(TYPE = CSV, ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)"
  ```

### ❌ **No Data Appears in Snowflake**
✅ **Solution:**
- Ensure the **S3 path is correct**.
- Verify files exist:
  ```bash
  aws s3 ls s3://febde2025/airflow_project/ # Use it if you have AWS CLI and access key
  ```
- Check Airflow logs for errors.

## 📬 Contact
For any issues, please raise an issue in the GitHub repository. 🚀
