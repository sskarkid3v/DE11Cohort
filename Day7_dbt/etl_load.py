import pandas as pd
from sqlalchemy import create_engine, text
import os

#define database connection
DATABASE_URL = "postgresql+psycopg2://demo_user:demo_pass@localhost:5432/salesdb"
engine = create_engine(DATABASE_URL)
DATA_DIR = "data"  # Directory where CSV files are stored


#define data directory
# create dictionary mapping tables to their respective CSV files
csv_map = {
    "departments": "departments.csv",
    "patients": "patients.csv",
    "doctors": "doctors.csv",
    "appointments": "appointments.csv",
    "treatments": "treatments.csv",
    "diagnoses": "diagnoses.csv"
}

#create a function that will create raw tables
def create_raw_tables():
    ddl_satements = {
        "departments": """
            CREATE TABLE IF NOT EXISTS departments (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
        """,
        "patients": """
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                gender TEXT,
                dob DATE,
                city TEXT
            );
        """,
        "doctors": """
            CREATE TABLE IF NOT EXISTS doctors (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                specialization VARCHAR(100),
                department_id INTEGER REFERENCES departments(id)
            );
        """,
        "appointments": """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY,
                patient_id INTEGER,
                doctor_id INTEGER,
                appointment_date DATE,
                status VARCHAR(20),
                department_id INTEGER
            );
        """,
        "treatments": """
            CREATE TABLE IF NOT EXISTS treatments (
                id INTEGER PRIMARY KEY,
                appointment_id INTEGER,
                treatment VARCHAR(100),
                cost DECIMAL(10, 2)
            );
        """,
        "diagnoses": """
            CREATE TABLE IF NOT EXISTS diagnoses (
                appointment_id INTEGER PRIMARY KEY,
                symptoms TEXT,
                diagnosis TEXT,
                notes TEXT
            );
        """
    } 
    with engine.begin() as conn:
        for ddl in ddl_satements.values():
            conn.execute(text(ddl))
        
#create a function to load csvs into their respective tables
def load_csv():
    for table, file in csv_map.items():
        df = pd.read_csv(os.path.join(DATA_DIR, file))
        df.to_sql(table, con=engine, if_exists='refresh', index=False)

#create a function to implement the kimball model
# CTAS statements to create the star schema tables
def create_kimball_model():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_patients CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dim_patients AS
            SELECT id as Patient_Id, name, dob, city,
            EXTRACT(YEAR FROM AGE(dob)) AS Age FROM patients"""))
        # add more dimensions and facts here as needed
 
 # create a function to implement the medallion architecture
# create silver and gold tables           
def create_medallion_model():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver_appointments CASCADE;"))
        conn.execute(text("""
            CREATE TABLE silver_appointments AS
            SELECT a.id AS Appointment_ID, a.patient_id, p.name as Patient_Name,
            p.gender, EXTRACT(YEAR FROM AGE(p.dob)) AS Patient_Age,
            a.doctor_id, d.name as doctor_name, d.specialization,
            dep.name as department_name, a.appointment_date, a.status
            FROM appointments a
            JOIN patients p ON a.patient_id = p.id
            JOIN doctors d ON a.doctor_id = d.id
            JOIN departments dep ON d.department_id = dep.id
            WHERE a.status = 'Completed'
        """))
        #add more models to silver layer as needed and also create gold layer on top
        
 #create a function to implement combined kimball and medallion architecture       
def create_combined_model():
    print("Creating combined model...")
   # create your silver tables for medallion
   # create your gold fact tables for medallion
   # create your dimension table for kimball
   # create your fact table for kimball

#create our main function to run the ETL process
if __name__ == "__main__":
    print("Creating raw tables...")
    create_raw_tables()
    print("Loading CSV files into raw tables...")
    load_csv()
    print("Creating Kimball model...")
    create_kimball_model()
    print("Creating Medallion model...")
    create_medallion_model()
    print("Creating combined model...")
    create_combined_model()
    print("ETL process completed successfully!")
    
# make it so that individual functions can be called instead of running the entire ETL process
# after successfully completing the ETL process, move the CSV files to a different directory named "processed_data"