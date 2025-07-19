# Software Engineering Project 3
# Name : Atharva Vaidya
# Student ID : aav6986

"""
Healthcare Data Transformation
Transforms legacy healthcare data into a snowflake schema
"""
import csv
import os
import datetime
import time
from collections import defaultdict
from typing import Dict, List, Any, Optional, Set

# Creating output directory if it doesn't exist
os.makedirs("output", exist_ok=True)

# Schema for each table
SCHEMAS = {
    "DimPatient": [
        "patient_id",
        "patient_first_name",
        "patient_last_name",
        "patient_date_of_birth",
        "patient_gender",
        "patient_address_line1",
        "patient_address_line2",
        "patient_city",
        "patient_state",
        "patient_zip",
        "patient_phone",
        "patient_email",
        "patient_status",
    ],
    "DimInsurance": [
        "insurance_id",
        "patient_id",
        "insurance_payer_name",
        "insurance_policy_number",
        "insurance_group_number",
        "insurance_plan_type",
    ],
    "DimBilling": [
        "billing_id",
        "insurance_id",
        "billing_amount_paid",
        "billing_total_charge",
        "billing_date",
        "billing_payment_status",
    ],
    "DimProvider": ["provider_id", "doctor_name", "doctor_title", "doctor_department"],
    "DimLocation": ["location_id", "clinic_name", "room_number"],
    "DimPrimaryDiagnosis": [
        "primary_diagnosis_id",
        "primary_diagnosis_code",
        "primary_diagnosis_desc",
    ],
    "DimSecondaryDiagnosis": [
        "secondary_diagnosis_id",
        "secondary_diagnosis_code",
        "secondary_diagnosis_desc",
    ],
    "DimTreatment": ["treatment_id", "treatment_code", "treatment_desc"],
    "DimPrescription": [
        "prescription_id",
        "prescription_drug_name",
        "prescription_dosage",
        "prescription_frequency",
        "prescription_duration_days",
    ],
    "DimLabOrder": [
        "lab_order_id",
        "lab_test_code",
        "lab_name",
        "lab_result_value",
        "lab_result_units",
        "lab_result_date",
    ],
    "FactVisit": [
        "visit_id",
        "patient_id",
        "insurance_id",
        "billing_id",
        "provider_id",
        "location_id",
        "primary_diagnosis_id",
        "secondary_diagnosis_id",
        "treatment_id",
        "prescription_id",
        "lab_order_id",
        "visit_datetime",
        "visit_type",
    ],
}

# Precompiled date formats for faster parsing
DATE_FORMATS = [
    ("%Y-%m-%d", lambda x: True),  # Standard ISO format
    ("%m/%d/%Y", lambda x: "/" in x),  # US format with slashes
    (
        "%d-%m-%Y",
        lambda x: "-" in x and len(x.split("-")) == 3,
    ),  # Day first with hyphens
    ("%Y/%m/%d", lambda x: "/" in x and len(x) >= 8),  # Year first with slashes
]

# Define threshold: visits >= this date mean "Active" status
# No end date needed since check is only for visits from 2022 onwards
ACTIVE_START_DATE = datetime.datetime(2022, 1, 1)


class DataProcessor:
    def __init__(self, batch_size: int = 100):
        # Data structures to store normalized data
        self.patients: Dict[str, Dict[str, Any]] = {}
        self.insurances: Dict[str, Dict[str, Any]] = {}
        self.billings: Dict[str, Dict[str, Any]] = {}
        self.providers: Dict[int, Dict[str, Any]] = {}
        self.locations: Dict[int, Dict[str, Any]] = {}
        self.primary_diagnoses: Dict[int, Dict[str, Any]] = {}
        self.secondary_diagnoses: Dict[int, Dict[str, Any]] = {}
        self.treatments: Dict[int, Dict[str, Any]] = {}
        self.prescriptions: Dict[str, Dict[str, Any]] = {}
        self.lab_orders: Dict[str, Dict[str, Any]] = {}
        self.visits: List[Dict[str, Any]] = []

        # Lookup dictionaries for deduplication
        self.provider_lookup: Dict[str, int] = {}
        self.location_lookup: Dict[str, int] = {}
        self.primary_diagnosis_lookup: Dict[str, int] = {}
        self.secondary_diagnosis_lookup: Dict[str, int] = {}
        self.treatment_lookup: Dict[str, int] = {}

        # Set batch size for processing
        self.batch_size = batch_size

        # Track processed visit IDs to avoid duplicates
        self.processed_visit_ids: Set[str] = set()

        # Track patient visit dates for determining status
        self.patient_visit_dates: Dict[str, List[datetime.datetime]] = defaultdict(list)

    def parse_date(self, date_str: str) -> str:
        """Parse date string into standard format with optimized format detection"""
        if not date_str:
            return ""

        try:
            # Skip formats that don't match the pattern
            for fmt, condition in DATE_FORMATS:
                if condition(date_str):
                    try:
                        return datetime.datetime.strptime(date_str, fmt).strftime(
                            "%Y-%m-%d"
                        )
                    except ValueError:
                        continue
            return date_str
        except Exception:
            return date_str

    def _parse_datetime(self, datetime_str: str) -> Optional[datetime.datetime]:
        """Parse datetime string into datetime object for comparison"""
        if not datetime_str:
            return None

        try:
            # Check for ISO format with time component
            if "T" in datetime_str or " " in datetime_str:
                # Handle ISO format or space-separated datetime
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]:
                    try:
                        return datetime.datetime.strptime(
                            datetime_str.split(".")[0], fmt
                        )
                    except ValueError:
                        continue

            # If no time component or above formats didn't match, try date-only formats
            date_str = self.parse_date(datetime_str)
            if date_str:
                return datetime.datetime.strptime(date_str, "%Y-%m-%d")
        except Exception as e:
            print(f"⚠️ Could not parse date '{datetime_str}': {e}")

        return None

    def process_batch(self, batch: List[Dict[str, str]]) -> None:
        """Process a batch of rows"""
        for row in batch:
            self._process_row(row)

    def _process_row(self, row: Dict[str, str]) -> None:
        """Process a single row of data"""
        # Extract fields once for efficiency
        patient_id = row["patient_id"]
        visit_id = row["visit_id"]
        visit_datetime = row["visit_datetime"]

        # Skip already processed visit IDs (deduplication)
        if visit_id in self.processed_visit_ids:
            return
        self.processed_visit_ids.add(visit_id)

        # Process patient dimensions only if new
        if patient_id not in self.patients:
            self._process_patient(row, patient_id)

        # Track visit date for patient status calculation
        visit_date = self._parse_datetime(visit_datetime)
        if visit_date:
            self.patient_visit_dates[patient_id].append(visit_date)
        else:
            print(
                f" ⚠️ Could not parse visit date '{visit_datetime}' for patient {patient_id}"
            )

        # Process related dimensions
        insurance_id = self._process_insurance(row, patient_id)
        billing_id = self._process_billing(row, insurance_id)
        provider_id = self._process_provider(row)
        location_id = self._process_location(row)
        primary_diagnosis_id = self._process_primary_diagnosis(row)
        secondary_diagnosis_id = self._process_secondary_diagnosis(row)
        treatment_id = self._process_treatment(row)
        prescription_id = self._process_prescription(row)
        lab_order_id = self._process_lab_order(row)

        # Add visit fact
        self.visits.append(
            {
                "visit_id": visit_id,
                "patient_id": patient_id,
                "insurance_id": insurance_id,
                "billing_id": billing_id,
                "provider_id": provider_id,
                "location_id": location_id,
                "primary_diagnosis_id": primary_diagnosis_id,
                "secondary_diagnosis_id": secondary_diagnosis_id,
                "treatment_id": treatment_id,
                "prescription_id": prescription_id,
                "lab_order_id": lab_order_id,
                "visit_datetime": visit_datetime,
                "visit_type": row["visit_type"],
            }
        )

    def _process_patient(self, row: Dict[str, str], patient_id: str) -> None:
        """Process patient data"""
        self.patients[patient_id] = {
            "patient_id": patient_id,
            "patient_first_name": row["patient_first_name"],
            "patient_last_name": row["patient_last_name"],
            "patient_date_of_birth": self.parse_date(row["patient_date_of_birth"]),
            "patient_gender": row["patient_gender"],
            "patient_address_line1": row["patient_address_line1"],
            "patient_address_line2": row["patient_address_line2"],
            "patient_city": row["patient_city"],
            "patient_state": row["patient_state"],
            "patient_zip": row["patient_zip"],
            "patient_phone": row["patient_phone"],
            "patient_email": row["patient_email"],
            "patient_status": "Active",  # Default status, will be updated later
        }

    def _process_insurance(self, row: Dict[str, str], patient_id: str) -> str:
        """Process insurance data"""
        insurance_id = row["insurance_id"]
        if insurance_id and insurance_id not in self.insurances:
            self.insurances[insurance_id] = {
                "insurance_id": insurance_id,
                "patient_id": patient_id,
                "insurance_payer_name": row["insurance_payer_name"],
                "insurance_policy_number": row["insurance_policy_number"],
                "insurance_group_number": row["insurance_group_number"],
                "insurance_plan_type": row["insurance_plan_type"],
            }
        return insurance_id

    def _process_billing(self, row: Dict[str, str], insurance_id: str) -> str:
        """Process billing data"""
        billing_id = row["billing_id"]
        if billing_id and billing_id not in self.billings:
            # Convert billing amounts to decimal (float) with safe conversion
            billing_amount_paid = self._safe_float(row["billing_amount_paid"])
            billing_total_charge = self._safe_float(row["billing_total_charge"])

            self.billings[billing_id] = {
                "billing_id": billing_id,
                "insurance_id": insurance_id,
                "billing_amount_paid": billing_amount_paid,
                "billing_total_charge": billing_total_charge,
                "billing_date": self.parse_date(row["billing_date"]),
                "billing_payment_status": row["billing_payment_status"],
            }
        return billing_id

    def _safe_float(self, value: str) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _process_provider(self, row: Dict[str, str]) -> int:
        """Process provider data with efficient lookup"""
        doctor_name = row["doctor_name"]
        doctor_title = row["doctor_title"]
        doctor_department = row["doctor_department"]

        # Skip processing if any required field is missing
        if not (doctor_name and doctor_title and doctor_department):
            return 0  # Return 0 for missing data

        # Create a hash key for lookup
        doctor_key = f"{doctor_name}|{doctor_title}|{doctor_department}"

        if doctor_key not in self.provider_lookup:
            provider_id = len(self.providers) + 1
            self.provider_lookup[doctor_key] = provider_id
            self.providers[provider_id] = {
                "provider_id": provider_id,
                "doctor_name": doctor_name,
                "doctor_title": doctor_title,
                "doctor_department": doctor_department,
            }
        return self.provider_lookup[doctor_key]

    def _process_location(self, row: Dict[str, str]) -> int:
        """Process location data with efficient lookup"""
        clinic_name = row["clinic_name"]
        room_number = row["room_number"]

        # Skip processing if any required field is missing
        if not (clinic_name and room_number):
            return 0  # Return 0 for missing data

        location_key = f"{clinic_name}|{room_number}"

        if location_key not in self.location_lookup:
            location_id = len(self.locations) + 1
            self.location_lookup[location_key] = location_id
            self.locations[location_id] = {
                "location_id": location_id,
                "clinic_name": clinic_name,
                "room_number": room_number,
            }
        return self.location_lookup[location_key]

    def _process_primary_diagnosis(self, row: Dict[str, str]) -> str:
        """Process primary diagnosis data"""
        primary_diagnosis_code = row.get("primary_diagnosis_code", "")
        primary_diagnosis_desc = row.get("primary_diagnosis_desc", "")

        if not (primary_diagnosis_code or primary_diagnosis_desc):
            return ""

        primary_diagnosis_key = f"{primary_diagnosis_code}|{primary_diagnosis_desc}"

        if primary_diagnosis_key not in self.primary_diagnosis_lookup:
            primary_diagnosis_id = len(self.primary_diagnoses) + 1
            self.primary_diagnosis_lookup[primary_diagnosis_key] = primary_diagnosis_id
            self.primary_diagnoses[primary_diagnosis_id] = {
                "primary_diagnosis_id": primary_diagnosis_id,
                "primary_diagnosis_code": primary_diagnosis_code,
                "primary_diagnosis_desc": primary_diagnosis_desc,
            }
        return str(self.primary_diagnosis_lookup[primary_diagnosis_key])

    def _process_secondary_diagnosis(self, row: Dict[str, str]) -> str:
        """Process secondary diagnosis data"""
        secondary_diagnosis_code = row.get("secondary_diagnosis_code", "")
        secondary_diagnosis_desc = row.get("secondary_diagnosis_desc", "")

        if not (secondary_diagnosis_code or secondary_diagnosis_desc):
            return ""

        secondary_diagnosis_key = (
            f"{secondary_diagnosis_code}|{secondary_diagnosis_desc}"
        )

        if secondary_diagnosis_key not in self.secondary_diagnosis_lookup:
            secondary_diagnosis_id = len(self.secondary_diagnoses) + 1
            self.secondary_diagnosis_lookup[secondary_diagnosis_key] = (
                secondary_diagnosis_id
            )
            self.secondary_diagnoses[secondary_diagnosis_id] = {
                "secondary_diagnosis_id": secondary_diagnosis_id,
                "secondary_diagnosis_code": secondary_diagnosis_code,
                "secondary_diagnosis_desc": secondary_diagnosis_desc,
            }
        return str(self.secondary_diagnosis_lookup[secondary_diagnosis_key])

    def _process_treatment(self, row: Dict[str, str]) -> str:
        """Process treatment data"""
        treatment_code = row.get("treatment_code", "")
        treatment_desc = row.get("treatment_desc", "")

        if not (treatment_code or treatment_desc):
            return ""

        treatment_key = f"{treatment_code}|{treatment_desc}"

        if treatment_key not in self.treatment_lookup:
            treatment_id = len(self.treatments) + 1
            self.treatment_lookup[treatment_key] = treatment_id
            self.treatments[treatment_id] = {
                "treatment_id": treatment_id,
                "treatment_code": treatment_code,
                "treatment_desc": treatment_desc,
            }
        return str(self.treatment_lookup[treatment_key])

    def _process_prescription(self, row: Dict[str, str]) -> str:
        """Process prescription data"""
        prescription_id = row.get("prescription_id", "")

        if not prescription_id:
            return ""

        if prescription_id not in self.prescriptions:
            # Only process if we have some valid data
            prescription_drug_name = row.get("prescription_drug_name", "")
            prescription_dosage = row.get("prescription_dosage", "")
            prescription_frequency = row.get("prescription_frequency", "")

            # Safe conversion for duration
            try:
                prescription_duration_days = int(
                    float(row.get("prescription_duration_days", 0))
                )
            except (ValueError, TypeError):
                prescription_duration_days = 0

            # Only add if we have at least one field with data
            if (
                prescription_drug_name
                or prescription_dosage
                or prescription_frequency
                or prescription_duration_days
            ):
                self.prescriptions[prescription_id] = {
                    "prescription_id": prescription_id,
                    "prescription_drug_name": prescription_drug_name,
                    "prescription_dosage": prescription_dosage,
                    "prescription_frequency": prescription_frequency,
                    "prescription_duration_days": prescription_duration_days,
                }
            else:
                # No valid data
                return ""

        return prescription_id

    def _process_lab_order(self, row: Dict[str, str]) -> str:
        """Process lab order data"""
        lab_order_id = row.get("lab_order_id", "")

        if not lab_order_id:
            return ""

        if lab_order_id not in self.lab_orders:
            # Check if we have some valid data
            lab_test_code = row.get("lab_test_code", "")
            lab_name = row.get("lab_name", "")
            lab_result_value = row.get("lab_result_value", "")
            lab_result_units = row.get("lab_result_units", "")
            lab_result_date = row.get("lab_result_date", "")

            # Only add if we have at least one field with data
            if (
                lab_test_code
                or lab_name
                or lab_result_value
                or lab_result_units
                or lab_result_date
            ):
                self.lab_orders[lab_order_id] = {
                    "lab_order_id": lab_order_id,
                    "lab_test_code": lab_test_code,
                    "lab_name": lab_name,
                    "lab_result_value": lab_result_value,
                    "lab_result_units": lab_result_units,
                    "lab_result_date": self.parse_date(lab_result_date),
                }
            else:
                # No valid data
                return ""

        return lab_order_id

    def _update_patient_statuses(self) -> None:
        """Update patient statuses based on visit dates"""
        print("\n🏁 Updating patient statuses based on visits from 2022 onwards\n")
        # time.sleep(1)  # 1 second delay
        active_count = 0
        inactive_count = 0

        for patient_id, visits in self.patient_visit_dates.items():
            # Check if patient has any visits in 2022 or later
            has_recent_visits = False

            for visit_date in visits:
                # Ensure visit_date is a datetime object for comparison
                # Check if visit is on or after Jan 1, 2022
                if visit_date and visit_date >= ACTIVE_START_DATE:
                    has_recent_visits = True
                    break

            # Update patient status based on visit activity
            if patient_id in self.patients:
                status = "Active" if has_recent_visits else "Inactive"
                self.patients[patient_id]["patient_status"] = status
                if status == "Active":
                    active_count += 1
                else:
                    inactive_count += 1

        # For any patients that don't have visit records at all, mark as Inactive
        for patient_id in self.patients:
            if patient_id not in self.patient_visit_dates:
                self.patients[patient_id]["patient_status"] = "Inactive"
                inactive_count += 1

    def process_data(self) -> None:
        """Process the legacy healthcare data CSV file with batch processing"""
        # Get the absolute path of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Get the parent directory (project root)
        project_root = os.path.dirname(script_dir)
        # Build the full path to the dataset
        dataset_path = os.path.join(
            project_root, "dataset", "legacy_healthcare_data.csv"
        )

        print(f" 🔍 dataset at: {dataset_path}")
        # time.sleep(1)  # 1 second delay

        # Process in batches for better memory usage
        with open(dataset_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            # Print column info
            if reader.fieldnames:
                print("\nAvailable columns in CSV file:")
                for column in reader.fieldnames:
                    print(f"  • {column}")
                    time.sleep(0.1)  # 0.1 second delay per column
                
                # Display total column count with a delay
                column_count = len(reader.fieldnames)
                print(f"\nColumn total count: {column_count}")
                time.sleep(0.2)  # 0.2 second delay for column count
            else:
                print("\nCould not read columns from the CSV.")

            # Process in batches
            batch = []
            total_rows = 0

            for row in reader:
                batch.append(row)
                total_rows += 1

                if len(batch) >= self.batch_size:
                    self.process_batch(batch)
                    print(f"  ◊ Processed {total_rows} rows...")
                    batch = []  # Clear the batch

            # Process any remaining rows
            if batch:
                self.process_batch(batch)

            # Update patient statuses based on visit dates
            self._update_patient_statuses()
            self.total_rows = total_rows

    def _get_patient_status_summary(self) -> str:
        """Get summary of patient statuses"""
        active_count = 0
        inactive_count = 0

        for patient in self.patients.values():
            if patient["patient_status"] == "Active":
                active_count += 1
            else:
                inactive_count += 1

        return f"Active: {active_count}, Inactive: {inactive_count}"

    def write_csv_files(self) -> None:
        """Write all tables to CSV files with efficient writing"""
        # Define table data mapping
        tables = {
            "DimPatient": list(self.patients.values()),
            "DimInsurance": list(self.insurances.values()),
            "DimBilling": list(self.billings.values()),
            "DimProvider": list(self.providers.values()),
            "DimLocation": list(self.locations.values()),
            "DimPrimaryDiagnosis": list(self.primary_diagnoses.values()),
            "DimSecondaryDiagnosis": list(self.secondary_diagnoses.values()),
            "DimTreatment": list(self.treatments.values()),
            "DimPrescription": list(self.prescriptions.values()),
            "DimLabOrder": list(self.lab_orders.values()),
            "FactVisit": self.visits,
        }

        # Write each table
        for table_name, data in tables.items():
            self._write_csv_optimized(table_name, data)

    def _write_csv_optimized(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Write a table to a CSV file with optimized writing"""
        if not data:
            print(f"Warning: No data for {table_name}")
            return

        schema = SCHEMAS[table_name]
        output_file = f"output/{table_name}.csv"

        # Use an in-memory buffer for faster IO
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=schema)
            writer.writeheader()

            # Filter rows in batches and write
            for row in data:
                # Optimize by doing a single comprehension
                filtered_row = {k: row.get(k, "") for k in schema}
                writer.writerow(filtered_row)

        print(f"✅ Created {table_name}.csv with {len(data)} rows")
        # time.sleep(0.4)  # 0.2 second delay after showing created file


def main():
    """Main execution function"""
    print("\n\n🏁 Starting healthcare data transformation...\n")
    # time.sleep(2)  # 2 second delay
    processor = DataProcessor(batch_size=5000)
    processor.process_data()
    processor.write_csv_files()
    print(f"\nPatient status summary: {processor._get_patient_status_summary()}")
    print("\n•••• Transformation complete. Files saved to 'output' directory. ••••\n")

    print(f"Total data rows processed: {processor.total_rows}")


if __name__ == "__main__":
    main()
