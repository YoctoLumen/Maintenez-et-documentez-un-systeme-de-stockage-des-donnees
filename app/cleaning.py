import os
import pandas as pd

def harmonisation(df):
    
    df.columns = df.columns.str.replace('-', '_').str.replace(' ', '_')
    df['Name'] = df['Name'].str.lower().str.title()
    
    return df

def clean_df(df):

    df = harmonisation(df)

    return df