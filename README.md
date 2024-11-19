Customer Loan Data Warehouse Proof of Concept
=====================================================

Overview
------------
This proof of concept (PoC) demonstrates the importation of customer and loan data from CSV and Excel files into a staging environment of an MS SQL database. The project showcases an effective dimensional model design for easy and fast reporting.

Project Structure
---------------------
The project consists of the following files and folders:
  - data/: Contains the source data files:
  - borrowers_data.xlsx: Customer or borrower information
  - loan_data.csv: Loan information
  - repayment_Data.csv: Loan repayment data
  - Schedule_Data.csv: Payment schedules
  - dags/: Contains Airflow DAG files for workflow orchestration
  - logs/: Stores daily data logs
  - sql/: Contains SQL queries:par_overdue_loans.sql: Calculates PAR Days (number of days a loan was not paid in full)
  - images/: Contains data model diagrams:
  
Data Model
--------------
Data Model Diagram
!

Technical Approach
----------------------
**Data Warehousing:** 
  - Utilized Slowly Changing Dimension (SCD) Type 1 to ensure the model always contains the most recent data.

**ETL Process:**
  - Bronze Layer: Files are processed and loaded into the database without transformations.
  - Silver Layer: Data type enforcements, referential integrity checks, and basic time tracking columns are added.
  - Update/Insert Strategy: Existing records are updated if matched; new records are inserted otherwise.
    
**Monitoring and Logging:**
Logging: Daily data logs are stored in the logs/ folder.
Monitoring: Utilizes Prometheus capabilities for application monitoring.

Data Mapping
----------------
Data Mapping Table
Source File	Target Table	Description
borrowers_data.xlsx	dim_borrowers	Customer information
loan_data.csv	fact_loans	Loan information
repayment_Data.csv	fact_repayments	Loan repayment data
Schedule_Data.csv	dim_schedules	Payment schedules

PAR Days Calculation
-------------------------
The par_overdue_loans.sql query calculates the PAR Days, which represents the number of days a loan was not paid in full. The calculation takes into account the amount_at_risk, which is the total amount owed by the customer at any given time.

Orchestration
-----------------
Airflow is used for workflow orchestration, with DAG files stored in the dags/ folder.

Prerequisites
-----------------
MS SQL database setup
Prometheus installation (for monitoring)
Airflow installation (for orchestration)

Usage
---------
Clone the repository.
Update database connection settings.
Run the ETL process.
Verify data importation and dimensional model.

Future Enhancements
----------------------
Implement data validation rules
Integrate with reporting tools (e.g., Power BI, Tableau)
Explore additional monitoring capabilities

Acknowledgments
------------------
[Insert acknowledgments or references]
