import os
import pandas as pd

def harmonisation(df):
    
   df_clean = df.copy()
    
    # 1. Normalisation des noms
    df_clean['Name'] = df_clean['Name'].str.strip().str.title()
    df_clean['Doctor'] = df_clean['Doctor'].str.strip().str.title()
    
    # 2. Nettoyage des hôpitaux
    df_clean['Hospital'] = df_clean['Hospital'].str.strip().str.replace(r',\s*$', '', regex=True)
    df_clean['Hospital'] = df_clean['Hospital'].str.replace(r'\s+', ' ', regex=True)
    
    # 3. Nettoyage de tous les champs texte
    text_columns = ['Gender', 'Blood Type', 'Medical Condition', 
                    'Insurance Provider', 'Admission Type', 'Medication', 'Test Results']
    for col in text_columns:
        df_clean[col] = df_clean[col].str.strip()
    
    # 4. Conversion des types numériques
    df_clean['Age'] = pd.to_numeric(df_clean['Age'], errors='coerce').astype('Int64')
    df_clean['Room Number'] = pd.to_numeric(df_clean['Room Number'], errors='coerce').astype('Int64')
    df_clean['Billing Amount'] = pd.to_numeric(df_clean['Billing Amount'], errors='coerce').round(2)
    
    # 5. Conversion des dates
    df_clean['Date of Admission'] = pd.to_datetime(df_clean['Date of Admission'], errors='coerce')
    df_clean['Discharge Date'] = pd.to_datetime(df_clean['Discharge Date'], errors='coerce')
    
    # 6. Normalisation du genre (capitalisation)
    df_clean['Gender'] = df_clean['Gender'].str.capitalize()
    
    # 7. Normalisation du groupe sanguin (majuscules)
    df_clean['Blood Type'] = df_clean['Blood Type'].str.upper()

    df_clean.to_csv('medical_data_cleaned.csv', index=False)
    return df_clean    

def clean_df(df):

    df_cleaned = harmonisation(df)


    return df
