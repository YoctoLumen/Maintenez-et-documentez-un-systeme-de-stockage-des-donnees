import pandas as pd

def PreMigration(df):

    # Liste des colonnes attendues
    expected_columns = [
        "Name", "Gender", "Blood_Type", "Age", "Medical_Condition",
        "Date_of_Admission", "Discharge_Date", "Admission_Type",
        "Room_Number", "Doctor", "Hospital", "Billing_Amount",
        "Insurance_Provider", "Medication", "Test_Results"
    ]

    def check_columns(df):
        missing_cols = [col for col in expected_columns if col not in df.columns]
        extra_cols = [col for col in df.columns if col not in expected_columns]
        if missing_cols:
            print(f"Colonnes manquantes : {missing_cols}")
        if extra_cols:
            print(f"Colonnes supplémentaires : {extra_cols}")
        if not missing_cols and not extra_cols:
            print("Toutes les colonnes attendues sont présentes.")

    def check_types(df):
        print("\nVérification des types de colonnes :")
        for col in expected_columns:
            if col in df.columns:
                print(f"{col} ({type(df[col].iloc[0])})")
            else:
                print(f"{col} absent dans le DataFrame.")

    def check_duplicates(df):
        duplicates = df[df.duplicated()]
        print(f"\nNombre de doublons : {len(duplicates)}")
        if len(duplicates) > 0:
            print(duplicates)

    def check_missing_values(df):
        missing = df.isnull().sum()
        print("\nValeurs manquantes par colonne :")
        print(missing[missing > 0])

    print("Vérification des données avant migration :")
    check_columns(df)
    check_types(df)
    check_duplicates(df)
    check_missing_values(df)