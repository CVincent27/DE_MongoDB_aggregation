import pandas as pd

def check_missing_birth_year(df):
    count_missing_birth = 0
    for index, row in df.iterrows():
        birth_year = row['birth_year']
        if pd.isna(birth_year) or birth_year == "":
            count_missing_birth += 1
            df.at[index, 'missing_year'] = True
        else:
            df.at[index, 'missing_year'] = False
    print(f"\nnb dates naissances manquantes dans la base {count_missing_birth}")
    return df

# remplace nan par la moyenne
def clean_birth_year(df):
    df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce')
    mean_birth_year = round(df['birth_year'].mean(skipna=True))
    #print(f"Moyenne birth_year : {mean_birth_year}")
    df.fillna({'birth_year': mean_birth_year}, inplace=True)
    df['birth_year'] = df['birth_year'].astype(int)
    return df
