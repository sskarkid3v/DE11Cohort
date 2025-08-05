import pandas as pd
from sqlalchemy import create_engine, text
import os
import shutil
import sys  # <-- used to accept CLI arguments

# Define database connection
DATABASE_URL = "postgresql+psycopg2://demo_user:demo_pass@localhost:5432/salesdb"
engine = create_engine(DATABASE_URL)

# Define data directories
DATA_DIR = "data"  # Directory where raw CSVs are stored
PROCESSED_DIR = "processed_data"  # Directory where processed CSVs will be moved


# CSV file mapping
csv_map = {
    "departments": "departments.csv",
    "patients": "patients.csv",
    "doctors": "doctors.csv",
    "appointments": "appointments.csv",
    "treatments": "treatments.csv",
    "diagnoses": "diagnoses.csv"
}

# Create raw tables
def create_raw_tables():
    ddl_statements = {
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
        for ddl in ddl_statements.values():
            conn.execute(text(ddl))


# Load CSVs into raw tables
def load_csv():
    for table, file in csv_map.items():
        df = pd.read_csv(os.path.join(DATA_DIR, file))
        df.to_sql(table, con=engine, if_exists='replace', index=False)

    # ✅ TASK 1: Move processed CSV files to a new directory called 'processed_data'
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    for file in csv_map.values():
        shutil.move(os.path.join(DATA_DIR, file), os.path.join(PROCESSED_DIR, file))


# Kimball Model
def create_kimball_model():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_patients CASCADE;"))
        conn.execute(text("""
            CREATE TABLE dim_patients AS
            SELECT id as Patient_Id, name, dob, city,
                   EXTRACT(YEAR FROM AGE(dob)) AS Age
            FROM patients;
        """))
        # ✅ TASK 2: Add other dimensions such as dim_doctors, dim_departments
        # ✅ TASK 3: Create fact_appointments or fact_treatments table following Kimball’s fact-dim model


# Medallion Model
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
            WHERE a.status = 'Completed';
        """))

        # ✅ TASK 4: Create silver_treatments and silver_diagnoses tables
        # ✅ TASK 5: Create a gold_appointments_summary table with aggregated insights (e.g., total treatments per doctor)


# Combined Kimball + Medallion Model
def create_combined_model():
    print("Creating combined model...")

    # ✅ TASK 6: First call medallion model logic (silver/gold)
    create_medallion_model()

    # ✅ TASK 7: Then call kimball model logic (fact/dim)
    create_kimball_model()

    # ✅ TASK 8: Add example where a gold fact table is reused to build a star schema (e.g., gold_appointments_summary used as fact_appointments in Kimball)


# ✅ TASK 9: Accept command-line arguments to run specific steps only
# Usage: python etl_script.py raw_tables | load_data | kimball | medallion | combined | all

def main():
    #make changes to the main function to accept command-line arguments to run specific steps only

if __name__ == "__main__":
    main()
