import pandas as pd
import datetime as dt

# Vérification si 2025 - birth year >= 13
clean_data_path = 'data/cleaned_data.json'
df = pd.read_json(clean_data_path, orient="records", lines=True)

df['check_age'] = False

def check_age(df):
    actual_year = dt.datetime.today().year
    df['check_age'] = df['birth_year'].apply(lambda x: actual_year - x < 13)
    minor_count = df['check_age'].sum()
    print(f"\n{minor_count} utilisateur ont - de 13 ans")
    #print(df[df['check_age']])
    return df

# ancienne version
#def check_age(df):
    #pd.to_datetime('today').normalize()
    actual_year = dt.datetime.today().strftime("%Y")
    actual_year = int(actual_year)
    #print(type(actual_year))

    for index, row in df.iterrows():
        birth_year = row['birth_year']
        if actual_year - birth_year < 13:
            df.at[index, 'check_age'] = True

    minor_count = df['check_age'].sum()
    #print(df)
    print(f"{minor_count} ont moins de 13 ans")
    print(df[df['check_age']])
    return df