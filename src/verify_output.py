"""
CSV Output Verification Script
Validates the generated CSV files against requirements
"""
import os
import csv
import sys
from typing import Dict, List, Set, Any

# Import schemas from main.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from main import SCHEMAS

def verify_csv_files():
    """Verify all generated CSV files meet requirements"""
    output_dir = "output"
    
    # Check if output directory exists
    if not os.path.exists(output_dir):
        print("❌ Error: Output directory not found!")
        return False
    
    # Get all CSV files in the output directory
    csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    
    # Check if we have exactly 11 CSV files
    if len(csv_files) != 11:
        print(f"❌ Error: Expected 11 CSV files, but found {len(csv_files)}")
        return False
    
    print(f"✅ Found {len(csv_files)} CSV files in the output directory")
    
    # Load original data for comparison
    original_data = load_original_data()
    if not original_data:
        print("❌ Error: Could not load original data for comparison")
        return False
    
    # Verify each CSV file against its schema
    all_valid = True
    for table_name, schema in SCHEMAS.items():
        file_name = f"{table_name}.csv"
        file_path = os.path.join(output_dir, file_name)
        
        if not os.path.exists(file_path):
            print(f"❌ Error: {file_name} is missing")
            all_valid = False
            continue
        
        # Verify column sequence matches schema
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader, None)
            
            if headers is None:
                print(f"❌ Error: {file_name} is empty or has no headers")
                all_valid = False
                continue
            
            # Check if headers match schema
            if headers != schema:
                print(f"❌ Error: Column sequence in {file_name} doesn't match schema")
                print(f"  Expected: {schema}")
                print(f"  Found: {headers}")
                all_valid = False
                continue
            
            # Count rows and verify data against original
            row_count = 0
            valid_records = 0
            invalid_records = 0
            
            # Track processed records to check for missing records later
            processed_ids = set()
            
            # Reset file pointer and skip header
            file.seek(0)
            next(reader)
            
            for row in reader:
                row_count += 1
                # Convert row to dict for easier comparison
                record = {schema[i]: value for i, value in enumerate(row) if i < len(schema)}
                
                # Check if record exists in original data
                if verify_record_in_original(table_name, record, original_data):
                    valid_records += 1
                    
                    # Track the ID of this record for missing record check
                    id_field = schema[0]  # First field is the ID
                    if id_field in record:
                        processed_ids.add(record[id_field])
                else:
                    invalid_records += 1
                    if invalid_records <= 5:  # Limit the number of errors shown
                        print(f"  ❌ Record not found in original data: {record}")
            
            # Print summary for this table
            print(f"✅ {file_name}: {row_count} rows, {valid_records} valid, {invalid_records} invalid")
            
            # Check for missing records from original data
            check_missing_records(table_name, processed_ids, original_data)
    
    # Verify primary key types
    primary_key_types = verify_primary_key_types(output_dir)
    if not primary_key_types:
        all_valid = False
    
    # Verify referential integrity
    ref_integrity = verify_referential_integrity(output_dir)
    if not ref_integrity:
        all_valid = False
    
    return all_valid

def load_original_data() -> Dict[str, List[Dict[str, Any]]]:
    """Load original data from legacy_healthcare_data.csv"""
    # Get the absolute path of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the parent directory (project root)
    project_root = os.path.dirname(script_dir)
    # Build the full path to the dataset
    dataset_path = os.path.join(project_root, "dataset", "legacy_healthcare_data.csv")
    
    print(f"🔍 Loading original data from: {dataset_path}")
    
    try:
        # Create a dictionary to store original data
        original_data = {
            "patients": {},
            "insurances": {},
            "billings": {},
            "providers": {},
            "locations": {},
            "primary_diagnoses": {},
            "secondary_diagnoses": {},
            "treatments": {},
            "prescriptions": {},
            "lab_orders": {},
            "visits": {}
        }
        
        # Read the original CSV file
        with open(dataset_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Store visit data
                visit_id = row.get('visit_id', '')
                if visit_id:
                    original_data["visits"][visit_id] = row
                
                # Store patient data
                patient_id = row.get('patient_id', '')
                if patient_id:
                    original_data["patients"][patient_id] = {
                        'patient_id': patient_id,
                        'patient_first_name': row.get('patient_first_name', ''),
                        'patient_last_name': row.get('patient_last_name', ''),
                        'patient_date_of_birth': row.get('patient_date_of_birth', ''),
                        'patient_gender': row.get('patient_gender', ''),
                        'patient_address_line1': row.get('patient_address_line1', ''),
                        'patient_address_line2': row.get('patient_address_line2', ''),
                        'patient_city': row.get('patient_city', ''),
                        'patient_state': row.get('patient_state', ''),
                        'patient_zip': row.get('patient_zip', ''),
                        'patient_phone': row.get('patient_phone', ''),
                        'patient_email': row.get('patient_email', ''),
                        # Note: patient_status is derived, not in original data
                    }
                
                # Store insurance data
                insurance_id = row.get('insurance_id', '')
                if insurance_id:
                    original_data["insurances"][insurance_id] = {
                        'insurance_id': insurance_id,
                        'patient_id': patient_id,
                        'insurance_payer_name': row.get('insurance_payer_name', ''),
                        'insurance_policy_number': row.get('insurance_policy_number', ''),
                        'insurance_group_number': row.get('insurance_group_number', ''),
                        'insurance_plan_type': row.get('insurance_plan_type', '')
                    }
                
                # Store billing data
                billing_id = row.get('billing_id', '')
                if billing_id:
                    original_data["billings"][billing_id] = {
                        'billing_id': billing_id,
                        'insurance_id': insurance_id,
                        'billing_amount_paid': row.get('billing_amount_paid', ''),
                        'billing_total_charge': row.get('billing_total_charge', ''),
                        'billing_date': row.get('billing_date', ''),
                        'billing_payment_status': row.get('billing_payment_status', '')
                    }
                
                # Store other dimensions based on composite keys
                doctor_key = f"{row.get('doctor_name', '')}|{row.get('doctor_title', '')}|{row.get('doctor_department', '')}"
                if all(part for part in doctor_key.split('|')):
                    original_data["providers"][doctor_key] = {
                        'doctor_name': row.get('doctor_name', ''),
                        'doctor_title': row.get('doctor_title', ''),
                        'doctor_department': row.get('doctor_department', '')
                    }
                
                location_key = f"{row.get('clinic_name', '')}|{row.get('room_number', '')}"
                if all(part for part in location_key.split('|')):
                    original_data["locations"][location_key] = {
                        'clinic_name': row.get('clinic_name', ''),
                        'room_number': row.get('room_number', '')
                    }
                
                # Store diagnosis data
                primary_diag_key = f"{row.get('primary_diagnosis_code', '')}|{row.get('primary_diagnosis_desc', '')}"
                if any(part for part in primary_diag_key.split('|')):
                    original_data["primary_diagnoses"][primary_diag_key] = {
                        'primary_diagnosis_code': row.get('primary_diagnosis_code', ''),
                        'primary_diagnosis_desc': row.get('primary_diagnosis_desc', '')
                    }
                
                secondary_diag_key = f"{row.get('secondary_diagnosis_code', '')}|{row.get('secondary_diagnosis_desc', '')}"
                if any(part for part in secondary_diag_key.split('|')):
                    original_data["secondary_diagnoses"][secondary_diag_key] = {
                        'secondary_diagnosis_code': row.get('secondary_diagnosis_code', ''),
                        'secondary_diagnosis_desc': row.get('secondary_diagnosis_desc', '')
                    }
                
                # Store treatment data
                treatment_key = f"{row.get('treatment_code', '')}|{row.get('treatment_desc', '')}"
                if any(part for part in treatment_key.split('|')):
                    original_data["treatments"][treatment_key] = {
                        'treatment_code': row.get('treatment_code', ''),
                        'treatment_desc': row.get('treatment_desc', '')
                    }
                
                # Store prescription data
                prescription_id = row.get('prescription_id', '')
                if prescription_id:
                    original_data["prescriptions"][prescription_id] = {
                        'prescription_id': prescription_id,
                        'prescription_drug_name': row.get('prescription_drug_name', ''),
                        'prescription_dosage': row.get('prescription_dosage', ''),
                        'prescription_frequency': row.get('prescription_frequency', ''),
                        'prescription_duration_days': row.get('prescription_duration_days', '')
                    }
                
                # Store lab order data
                lab_order_id = row.get('lab_order_id', '')
                if lab_order_id:
                    original_data["lab_orders"][lab_order_id] = {
                        'lab_order_id': lab_order_id,
                        'lab_test_code': row.get('lab_test_code', ''),
                        'lab_name': row.get('lab_name', ''),
                        'lab_result_value': row.get('lab_result_value', ''),
                        'lab_result_units': row.get('lab_result_units', ''),
                        'lab_result_date': row.get('lab_result_date', '')
                    }
        
        print(f"✅ Successfully loaded original data with {len(original_data['visits'])} visits")
        return original_data
    
    except Exception as e:
        print(f"❌ Error loading original data: {str(e)}")
        return None

def verify_record_in_original(table_name: str, record: Dict[str, Any], original_data: Dict[str, Dict[str, Any]]) -> bool:
    """Verify if a record exists in the original data"""
    # Map table names to original data keys
    table_map = {
        "DimPatient": "patients",
        "DimInsurance": "insurances",
        "DimBilling": "billings",
        "FactVisit": "visits"
    }
    
    # For tables with direct ID mapping
    if table_name in table_map:
        data_key = table_map[table_name]
        id_field = list(record.keys())[0]  # First field is the ID
        record_id = record[id_field]
        
        # Special case for patient status which is derived
        if table_name == "DimPatient" and "patient_status" in record:
            # Create a copy of the record without status for comparison
            record_copy = record.copy()
            record_copy.pop("patient_status", None)
            
            # Check if the patient exists and all other fields match
            if record_id in original_data[data_key]:
                orig_record = original_data[data_key][record_id]
                for key, value in record_copy.items():
                    if key in orig_record and orig_record[key] != value:
                        return False
                return True
        
        # For other direct ID tables
        elif record_id in original_data[data_key]:
            return True
    
    # For tables with composite keys
    elif table_name in ["DimProvider", "DimLocation", "DimPrimaryDiagnosis", "DimSecondaryDiagnosis", "DimTreatment"]:
        # Map table names to original data keys and fields
        composite_map = {
            "DimProvider": ("providers", ["doctor_name", "doctor_title", "doctor_department"]),
            "DimLocation": ("locations", ["clinic_name", "room_number"]),
            "DimPrimaryDiagnosis": ("primary_diagnoses", ["primary_diagnosis_code", "primary_diagnosis_desc"]),
            "DimSecondaryDiagnosis": ("secondary_diagnoses", ["secondary_diagnosis_code", "secondary_diagnosis_desc"]),
            "DimTreatment": ("treatments", ["treatment_code", "treatment_desc"])
        }
        
        data_key, fields = composite_map[table_name]
        
        # Create composite key from record
        composite_values = [record.get(field, '') for field in fields if field in record]
        if not composite_values or not any(composite_values):
            return False
            
        composite_key = "|".join(composite_values)
        
        # Check if any original record contains this composite key
        for key in original_data[data_key]:
            if composite_key in key:
                return True
    
    # For prescription and lab order tables
    elif table_name in ["DimPrescription", "DimLabOrder"]:
        id_field = list(record.keys())[0]  # First field is the ID
        record_id = record[id_field]
        
        data_key = "prescriptions" if table_name == "DimPrescription" else "lab_orders"
        
        if record_id in original_data[data_key]:
            return True
    
    return False

def verify_primary_key_types(output_dir: str) -> bool:
    """Verify primary key types (string or integer) for all tables"""
    # Define primary key column for each table
    primary_keys = {
        "DimPatient": "patient_id",
        "DimInsurance": "insurance_id",
        "DimBilling": "billing_id",
        "DimProvider": "provider_id",
        "DimLocation": "location_id",
        "DimPrimaryDiagnosis": "primary_diagnosis_id",
        "DimSecondaryDiagnosis": "secondary_diagnosis_id",
        "DimTreatment": "treatment_id",
        "DimPrescription": "prescription_id",
        "DimLabOrder": "lab_order_id",
        "FactVisit": "visit_id"
    }
    
    all_valid = True
    
    for table, pk_column in primary_keys.items():
        file_path = os.path.join(output_dir, f"{table}.csv")
        
        if not os.path.exists(file_path):
            continue  # Skip if file doesn't exist (already reported)
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Check first row to determine primary key type
            try:
                first_row = next(reader, None)
                if first_row and pk_column in first_row:
                    pk_value = first_row[pk_column]
                    
                    # Check if value is integer or string
                    try:
                        int(pk_value)
                        pk_type = "integer"
                    except ValueError:
                        pk_type = "string"
                    
                    print(f"✅ {table}: Primary key '{pk_column}' is {pk_type} type")
                else:
                    print(f"❌ Error: Primary key column '{pk_column}' not found in {table}")
                    all_valid = False
            except Exception as e:
                print(f"❌ Error checking primary key in {table}: {str(e)}")
                all_valid = False
    
    return all_valid

def verify_referential_integrity(output_dir: str) -> bool:
    """Verify referential integrity between fact and dimension tables"""
    # Define foreign key relationships (table: {fk_column: referenced_table})
    foreign_keys = {
        "FactVisit": {
            "patient_id": "DimPatient",
            "insurance_id": "DimInsurance",
            "billing_id": "DimBilling",
            "provider_id": "DimProvider",
            "location_id": "DimLocation",
            "primary_diagnosis_id": "DimPrimaryDiagnosis",
            "secondary_diagnosis_id": "DimSecondaryDiagnosis",
            "treatment_id": "DimTreatment",
            "prescription_id": "DimPrescription",
            "lab_order_id": "DimLabOrder"
        }
    }
    
    # Load primary keys from dimension tables
    primary_keys: Dict[str, Set[str]] = {}
    for table in SCHEMAS.keys():
        if table == "FactVisit":
            continue
            
        pk_column = SCHEMAS[table][0]  # Assuming first column is primary key
        file_path = os.path.join(output_dir, f"{table}.csv")
        
        if not os.path.exists(file_path):
            continue
            
        primary_keys[table] = set()
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if pk_column in row and row[pk_column]:
                    primary_keys[table].add(row[pk_column])
    
    # Check foreign keys in fact table
    all_valid = True
    for table, fk_relations in foreign_keys.items():
        file_path = os.path.join(output_dir, f"{table}.csv")
        
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                for fk_column, ref_table in fk_relations.items():
                    # Skip empty foreign keys (allowed in snowflake schema)
                    if fk_column not in row or not row[fk_column]:
                        continue
                        
                    # Check if foreign key exists in referenced table
                    if ref_table in primary_keys and row[fk_column] not in primary_keys[ref_table]:
                        print(f"❌ Referential integrity error in {table} row {row_num}: "
                              f"{fk_column}='{row[fk_column]}' not found in {ref_table}")
                        all_valid = False
    
    if all_valid:
        print("✅ Referential integrity checks passed")
    
    return all_valid

def check_missing_records(table_name: str, processed_ids: Set[str], original_data: Dict[str, Dict[str, Any]]) -> None:
    """Check if any records from original data are missing in the generated files"""
    # Map table names to original data keys
    table_map = {
        "DimPatient": "patients",
        "DimInsurance": "insurances",
        "DimBilling": "billings",
        "FactVisit": "visits",
        "DimPrescription": "prescriptions",
        "DimLabOrder": "lab_orders"
    }
    
    # Only check tables with direct ID mapping
    if table_name not in table_map:
        return
    
    data_key = table_map[table_name]
    original_ids = set(original_data[data_key].keys())
    
    # For tables with composite keys or derived IDs, we can't easily check for missing records
    if table_name in ["DimProvider", "DimLocation", "DimPrimaryDiagnosis", 
                      "DimSecondaryDiagnosis", "DimTreatment"]:
        return
    
    # Find missing records
    missing_ids = original_ids - processed_ids
    
    if missing_ids:
        print(f"  ⚠️ {table_name} is missing {len(missing_ids)} records from original data")
        if len(missing_ids) <= 5:  # Show only first 5 missing IDs
            for missing_id in list(missing_ids)[:5]:
                print(f"    - Missing ID: {missing_id}")
        else:
            print(f"    - First 5 missing IDs: {list(missing_ids)[:5]}")
    else:
        print(f"  ✅ {table_name} contains all records from original data")

if __name__ == "__main__":
    print("\n🔍 Verifying CSV output files...\n")
    
    if verify_csv_files():
        print("\n✅ All verification checks passed! Your CSV files meet the requirements.")
    else:
        print("\n❌ Some verification checks failed. Please review the errors above.")