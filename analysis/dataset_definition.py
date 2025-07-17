from ehrql import create_dataset, codelist_from_csv
from ehrql.tables.core import patients, clinical_events

# Create the dataset
dataset = create_dataset()

# ---. Load Pregnancy-Related Codelists ---
codelist_files = {
    "pregnancy_test": "codelists/Local/A1_pregnancy_test.csv",
    "booking_visit": "codelists/Local/A2_booking_visit.csv", 
    "dating_scan": "codelists/Local/A3_dating_scan.csv", 
    "antenatal_screening": "codelists/Local/A4_antenatal_screening.csv", 
    "antenatal_risk": "codelists/Local/A5_risk_assessment.csv", 
    "antenatal_procedures": "codelists/Local/A6_antenatal_procedures.csv",
    "pregnancy_conditions": "codelists/Local/B1_live_birth.csv", 
    "pregnancy_complications": "codelists/Local/C4_preeclampsia.csv",
}


# Load codelists
codelists = {}
for k, v in codelist_files.items():
    try:
        codelists[k] = codelist_from_csv(v, column="code")
    except Exception as e:
        print(f"Error loading codelist {k} from {v}: {str(e)}")
        raise


# --- . Create Dataset and Define Population ---
dataset.age = patients.age_on("2020-03-31")
dataset.sex = patients.sex
dataset.define_population((dataset.age >= 14) & (dataset.age < 50) & (dataset.sex == "female"))

##Early pregnancy events##

# Define a generic function to get the next event date after a given date for any event set
def next_event_date_after(events, date):
    return (
        events
        .where(events.date > date)
        .first_for_patient()
        .date
    )

# 1. Get all pregnancy test events, sorted by date
pregnancy_test_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["pregnancy_test"])) .sort_by(clinical_events.date)

# 3. Extract up to 3 event dates (repeat as needed)
dataset.pregnancy_test_event_1 = pregnancy_test_events.first_for_patient().date
dataset.pregnancy_test_event_2 = next_event_date_after(pregnancy_test_events, dataset.pregnancy_test_event_1)
dataset.pregnancy_test_event_3 = next_event_date_after(pregnancy_test_events, dataset.pregnancy_test_event_2)

# 4. Get all booking visit events, sorted by date
booking_visit_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["booking_visit"])
).sort_by(clinical_events.date)

# 6. Extract up to 3 event dates (repeat as needed)
dataset.booking_visit_event_1 = booking_visit_events.first_for_patient().date
dataset.booking_visit_event_2 = next_event_date_after(booking_visit_events, dataset.booking_visit_event_1)
dataset.booking_visit_event_3 = next_event_date_after(booking_visit_events, dataset.booking_visit_event_2)

# 7. Get all scanning visit events, sorted by date
dating_scan_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["dating_scan"])
).sort_by(clinical_events.date)

# 9. Extract up to 3 event dates (repeat as needed)
dataset.dating_scan_event_1 = dating_scan_events.first_for_patient().date
dataset.dating_scan_event_2 = next_event_date_after(dating_scan_events, dataset.dating_scan_event_1)
dataset.dating_scan_event_3 = next_event_date_after(dating_scan_events, dataset.dating_scan_event_2)

##Antenatal care##

# 1. Get all antenatal screening events, sorted by date
antenatal_screening_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_screening"])
).sort_by(clinical_events.date)

# 3. Extract up to 3 event dates (repeat as needed)
dataset.antenatal_screening_event_1 = antenatal_screening_events.first_for_patient().date
dataset.antenatal_screening_event_2 = next_event_date_after(antenatal_screening_events, dataset.antenatal_screening_event_1)
dataset.antenatal_screening_event_3 = next_event_date_after(antenatal_screening_events, dataset.antenatal_screening_event_2)

# 4. Get all antenatal risk events, sorted by date
antenatal_risk_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_risk"])
).sort_by(clinical_events.date)

# 6. Extract up to 3 event dates (repeat as needed)
dataset.antenatal_risk_event_1 = antenatal_risk_events.first_for_patient().date
dataset.antenatal_risk_event_2 = next_event_date_after(antenatal_risk_events, dataset.antenatal_risk_event_1)
dataset.antenatal_risk_event_3 = next_event_date_after(antenatal_risk_events, dataset.antenatal_risk_event_2)

# 7. Get all antenatal procedure events, sorted by date
antenatal_procedure_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_procedures"])
).sort_by(clinical_events.date)

# 9. Extract up to 3 event dates (repeat as needed)
dataset.antenatal_procedure_event_1 = antenatal_procedure_events.first_for_patient().date
dataset.antenatal_procedure_event_2 = next_event_date_after(antenatal_procedure_events, dataset.antenatal_procedure_event_1)
dataset.antenatal_procedure_event_3 = next_event_date_after(antenatal_procedure_events, dataset.antenatal_procedure_event_2)

##Pregnancy conditions##
# 10. Get all pregnancy condition events, sorted by date

pregnancy_conditions_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["pregnancy_conditions"])
).sort_by(clinical_events.date)

# 12. Extract up to 3 event dates (repeat as needed)
dataset.pregnancy_conditions_event_1 = pregnancy_conditions_events.first_for_patient().date
dataset.pregnancy_conditions_event_2 = next_event_date_after(pregnancy_conditions_events, dataset.pregnancy_conditions_event_1)
dataset.pregnancy_conditions_event_3 = next_event_date_after(pregnancy_conditions_events, dataset.pregnancy_conditions_event_2)

##Pregnancy complications##

# 13. Get all pregnancy complications events, sorted by date

pregnancy_complications_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["pregnancy_complications"])
).sort_by(clinical_events.date)

# 15. Extract up to 3 event dates (repeat as needed)
dataset.pregnancy_complications_event_1 = pregnancy_complications_events.first_for_patient().date
dataset.pregnancy_complications_event_2 = next_event_date_after(pregnancy_complications_events, dataset.pregnancy_complications_event_1)
dataset.pregnancy_complications_event_3 = next_event_date_after(pregnancy_complications_events, dataset.pregnancy_complications_event_2)

# Configure dummy data for testing
dataset.configure_dummy_data(
    population_size=100
) 


