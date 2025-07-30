#!/usr/bin/env python3
import random
import time
from datetime import datetime
import pyodbc

# Database configuration
DB_CONFIG = {
    "driver": "ODBC Driver 18 for SQL Server",
    "server": "localhost\\SQLEXPRESS",
    "database": "Transactions_DB",
    "trusted_connection": "yes",
    "trust_server_certificate": "yes"
}

# Transaction configuration - slow generation
SLEEP_INTERVAL = (5.0, 15.0)  # 5-15 seconds between transactions

# Data templates
CHANNELS = ["TBLACQ9777", "HYP_RETAIL", "TBLACQ9900", "VISAACQ123"]
INTERFACES = ["STIP"]
MTI_TYPES = ["1614", "1110", "1210"]
TRANSACTION_TYPES = ["PIN-CHANGE", "BALINQ", "CASH", "PURCHASE"]
CURRENCIES = ["BDT", "USD", "EUR"]
RESPONSE_CODES = ["600", "000"]
APPROVAL_CODES = ["918133", "201760", "375836", "169856", "354585", "112962"]
PRODUCTS = ["VCTBL-VISA-ONUS", "MSTP-MASTERCARD", "AMEX-GOLD"]
TERMINAL_NAMES = ["DHANMONDI BRANCH ATM", "TBL TEST MERCHANT POS", "GULSHAN BRANCH ATM"]
ORIGINATOR_TYPES = ["TBLACQ9777", "HYP_RETAIL"]
TERMINAL_COUNTRIES = ["050"]
TERMINAL_LOCATIONS = ["Dhaka BD", "Chittagong BD", "Khulna BD"]
POS_ENTRY_MODES = ["511201513146600", "511201513146000", "510101513144000"]
DECLINE_REASONS = [
    "Accepted: Administrative info accepted",
    "Approved",
    "Decline: Withdrawal amount limit exceeded"
]
MCC_CODES = ["6011", "5311", "5812"]
ERRORS = ["", "SL_POS_CASH_DAILY", "SL_POS_CASH_WEEKLY"]

def generate_card_number():
    prefix = random.choice(["4", "5"])
    return f"{prefix}{random.randint(100000000000, 999999999999):012d}"

def mask_card(number):
    return f"{number[:6]}******{number[-4:]}"

def create_db_connection():
    conn_str = (
        f"Driver={DB_CONFIG['driver']};"
        f"Server={DB_CONFIG['server']};"
        f"Database={DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        f"TrustServerCertificate={DB_CONFIG['trust_server_certificate']};"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print(f"Database connection error: {e}")
        return None

def initialize_database(conn):
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TransactionLog')
        BEGIN
            CREATE TABLE TransactionLog (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                [Time] DATETIME NOT NULL,
                Channel NVARCHAR(50),
                Interface NVARCHAR(20),
                MTI NVARCHAR(10),
                [Card No] NVARCHAR(50),
                [Transaction Type] NVARCHAR(50),
                [Transaction Amount] DECIMAL(18, 2),
                Currency NVARCHAR(10),
                [Approval Code] NVARCHAR(20),
                [Response Code] NVARCHAR(20),
                RRN NVARCHAR(50),
                Product NVARCHAR(100),
                [Terminal Name] NVARCHAR(100),
                [Originator Type] NVARCHAR(50),
                [Terminal Country] NVARCHAR(50),
                [Terminal Location] NVARCHAR(100),
                [Card Holder Billing Amount] DECIMAL(18, 2),
                [Billing Currency] NVARCHAR(10),
                [POS Entry Mode] NVARCHAR(50),
                [Decline Reason] NVARCHAR(200),
                MCC NVARCHAR(10),
                Error NVARCHAR(100)
            )
            PRINT 'TransactionLog table created successfully'
        END
    """)
    conn.commit()

def generate_transaction():
    timestamp = datetime.now()
    transaction_type = random.choice(TRANSACTION_TYPES)
    
    # Transaction amount handling
    if transaction_type in ["BALINQ", "PIN-CHANGE"]:
        transaction_amount = 0.0
        billing_amount = 0.0
        currency = random.choice(CURRENCIES) if transaction_type == "PIN-CHANGE" else "NON"
    else:
        transaction_amount = float(random.randint(1, 1000) * 100)
        billing_amount = transaction_amount
        currency = random.choice(CURRENCIES)
    
    decline_reason = random.choice(DECLINE_REASONS)
    error = random.choice(ERRORS[1:]) if "Decline" in decline_reason else ""
    
    return {
        'Time': timestamp,
        'Channel': random.choice(CHANNELS),
        'Interface': random.choice(INTERFACES),
        'MTI': random.choice(MTI_TYPES),
        'Card No': mask_card(generate_card_number()),
        'Transaction Type': transaction_type,
        'Transaction Amount': transaction_amount,
        'Currency': currency,
        'Approval Code': random.choice(APPROVAL_CODES),
        'Response Code': random.choice(RESPONSE_CODES),
        'RRN': f"51461{random.randint(1000000, 9999999)}",
        'Product': random.choice(PRODUCTS),
        'Terminal Name': random.choice(TERMINAL_NAMES),
        'Originator Type': random.choice(ORIGINATOR_TYPES),
        'Terminal Country': random.choice(TERMINAL_COUNTRIES),
        'Terminal Location': random.choice(TERMINAL_LOCATIONS),
        'Card Holder Billing Amount': billing_amount,
        'Billing Currency': currency,
        'POS Entry Mode': random.choice(POS_ENTRY_MODES),
        'Decline Reason': decline_reason,
        'MCC': random.choice(MCC_CODES),
        'Error': error
    }

def insert_transaction(conn, transaction):
    cursor = conn.cursor()
    
    # Fixed the query parameter count (21 parameters now)
    query = """
        INSERT INTO TransactionLog (
            [Time], Channel, Interface, MTI, [Card No], [Transaction Type],
            [Transaction Amount], Currency, [Approval Code], [Response Code],
            RRN, Product, [Terminal Name], [Originator Type], [Terminal Country],
            [Terminal Location], [Card Holder Billing Amount], [Billing Currency],
            [POS Entry Mode], [Decline Reason], MCC
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.execute(query, (
            transaction['Time'],
            transaction['Channel'],
            transaction['Interface'],
            transaction['MTI'],
            transaction['Card No'],
            transaction['Transaction Type'],
            transaction['Transaction Amount'],
            transaction['Currency'],
            transaction['Approval Code'],
            transaction['Response Code'],
            transaction['RRN'],
            transaction['Product'],
            transaction['Terminal Name'],
            transaction['Originator Type'],
            transaction['Terminal Country'],
            transaction['Terminal Location'],
            transaction['Card Holder Billing Amount'],
            transaction['Billing Currency'],
            transaction['POS Entry Mode'],
            transaction['Decline Reason'],
            transaction['MCC']
            # Removed Error field from insert as it's not in the table
        ))
        conn.commit()
        print(f"Transaction added to database at {datetime.now().strftime('%H:%M:%S')}")
    except pyodbc.Error as e:
        print(f"Error inserting transaction: {e}")
        conn.rollback()

def main():
    conn = create_db_connection()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    initialize_database(conn)
    print("Transaction processor started. Press Ctrl+C to stop...")
    
    try:
        while True:
            transaction = generate_transaction()
            insert_transaction(conn, transaction)
            time.sleep(random.uniform(*SLEEP_INTERVAL))
    except KeyboardInterrupt:
        print("\nStopping transaction processor...")
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()