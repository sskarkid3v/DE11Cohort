
import random
import csv
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path

fake = Faker()
output_dir = Path("/home/subhan/data_modelling")
output_dir.mkdir(parents=True, exist_ok=True)

NUM_PATIENTS = 200
NUM_DOCTORS = 50
NUM_APPOINTMENTS = 1000
NUM_TREATMENTS = 800

departments = [
    {"id": i+1, "name": dept}
    for i, dept in enumerate([
        "Cardiology", "Neurology", "Orthopedics", "Pediatrics",
        "Oncology", "Radiology", "Dermatology", "ENT",
        "Psychiatry", "General Medicine"
    ])
]

patients = []
for pid in range(1, NUM_PATIENTS + 1):
    dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
    patients.append({
        "id": pid,
        "name": fake.name(),
        "gender": random.choice(["Male", "Female", "Other"]),
        "dob": dob.strftime("%Y-%m-%d"),
        "city": fake.city()
    })

doctors = []
for did in range(1, NUM_DOCTORS + 1):
    dept = random.choice(departments)
    doctors.append({
        "id": did,
        "name": fake.name(),
        "specialization": dept["name"],
        "department_id": dept["id"]
    })

appointments = []
for aid in range(1, NUM_APPOINTMENTS + 1):
    patient = random.choice(patients)
    doctor = random.choice(doctors)
    date = fake.date_between(start_date='-1y', end_date='today')
    appointments.append({
        "id": aid,
        "patient_id": patient["id"],
        "doctor_id": doctor["id"],
        "appointment_date": date.strftime("%Y-%m-%d"),
        "status": random.choice(["Completed", "Cancelled", "No Show"]),
        "department_id": doctor["department_id"]
    })

treatments = []
for tid in range(1, NUM_TREATMENTS + 1):
    aid = random.randint(1, NUM_APPOINTMENTS)
    treatments.append({
        "id": tid,
        "appointment_id": aid,
        "treatment": random.choice(["MRI", "X-Ray", "Physiotherapy", "Surgery", "Medication"]),
        "cost": round(random.uniform(100, 1500), 2)
    })

diagnoses = []
for app in appointments:
    diagnoses.append({
        "appointment_id": app["id"],
        "symptoms": fake.sentence(nb_words=5),
        "diagnosis": random.choice([
            "Hypertension", "Diabetes", "Fracture", "Migraine", "Asthma",
            "Skin Rash", "Depression", "Allergy", "Infection", "Back Pain"
        ]),
        "notes": fake.paragraph(nb_sentences=2)
    })

def write_csv(filename, rows, headers):
    with open(output_dir / filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

write_csv("departments.csv", departments, ["id", "name"])
write_csv("patients.csv", patients, ["id", "name", "gender", "dob", "city"])
write_csv("doctors.csv", doctors, ["id", "name", "specialization", "department_id"])
write_csv("appointments.csv", appointments, ["id", "patient_id", "doctor_id", "appointment_date", "status", "department_id"])
write_csv("treatments.csv", treatments, ["id", "appointment_id", "treatment", "cost"])
write_csv("diagnoses.csv", diagnoses, ["appointment_id", "symptoms", "diagnosis", "notes"])

print("✅ CSV files generated in:", output_dir.resolve())
