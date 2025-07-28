import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()
fake = Faker()

# -------------------------------
# Define Tables
# -------------------------------

class Customer(Base):
    __tablename__ = 'dim_customer'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    dob = Column(DateTime)
    branch_id = Column(Integer)
    city = Column(String)

class Account(Base):
    __tablename__ = 'dim_account'
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('dim_customer.id'))
    type = Column(String)
    opened_on = Column(DateTime)

class Branch(Base):
    __tablename__ = 'dim_branch'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    location = Column(String)

class Transaction(Base):
    __tablename__ = 'fact_transactions'
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('dim_account.id'))
    transaction_date = Column(DateTime)
    amount = Column(Float)
    transaction_type = Column(String)

# -------------------------------
# Create Engine and Session
# -------------------------------

engine = create_engine("postgresql+psycopg2://demo_user:demo_pass@localhost:5432/salesdb")
Session = sessionmaker(bind=engine)
session = Session()

# -------------------------------
# Generate Sample Data
# -------------------------------

def create_branch_data(n=10):
    branches = [Branch(name=f"Branch {i}", location=fake.city()) for i in range(1, n+1)]
    session.add_all(branches)
    session.commit()

def create_customers(n=10000):
    branches = session.query(Branch).all()
    customers = [
        Customer(
            name=fake.name(),
            dob=fake.date_of_birth(minimum_age=18, maximum_age=65),
            branch_id=random.choice(branches).id,
            city=fake.city()
        ) for _ in range(n)
    ]
    session.add_all(customers)
    session.commit()

def create_accounts(n=15000):
    customers = session.query(Customer).all()
    acc_types = ['Savings', 'Current', 'Fixed Deposit']
    accounts = [
        Account(
            customer_id=random.choice(customers).id,
            type=random.choice(acc_types),
            opened_on=fake.date_this_decade()
        ) for _ in range(n)
    ]
    session.add_all(accounts)
    session.commit()

def create_transactions(n=500000):
    accounts = session.query(Account).all()
    tx_types = ['Credit', 'Debit']
    transactions = []
    for _ in range(n):
        tx_date = datetime.now() - timedelta(days=random.randint(0, 365))
        transactions.append(
            Transaction(
                account_id=random.choice(accounts).id,
                transaction_date=tx_date,
                amount=round(random.uniform(10, 5000), 2),
                transaction_type=random.choice(tx_types)
            )
        )
    session.add_all(transactions)
    session.commit()

# -------------------------------
# Run Everything
# -------------------------------

if __name__ == "__main__":
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    print("Populating demo data...")
    create_branch_data()
    create_customers()
    create_accounts()
    create_transactions()
    print("✅ Done")
