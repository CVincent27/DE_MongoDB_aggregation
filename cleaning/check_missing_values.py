def data_type(df):
    if not df.empty:
        sample_doc = df.iloc[3]
        print("\nTypes colonnes:")
        for key, value in sample_doc.items():
            # __name__ pour recup le nom du type natif Python
            type_name = type(value).__name__ 
            print(f"{key}: {type_name}")
    else:
        print("Pas de données à analyser")

def missing_values(df):
    print("\nvaleurs manquantes par col :")
    print(df.isnull().sum())

def find_duplicates(df):
    cols_to_check = ['start_time', 'stop_time', 'bikeid']
    # Repérer lignes dupliquées
    duplicates = df[df.duplicated(subset=cols_to_check, keep=False)]

    if duplicates.empty:
        print("pas de doublon")
    else:
        duplicates_sorted = duplicates.sort_values(by='bikeid')
        print(f"Doublons trouvés: {len(duplicates_sorted)}")
        #print(duplicates_sorted)

    return duplicates

def drop_duplicates(df):
    cols_to_check = ['start_time', 'stop_time', 'bikeid']
    before_count = len(df)
    df.drop_duplicates(subset=cols_to_check, inplace=True)
    after_count = len(df)
    removed = before_count - after_count
    print(f"Doublons supprimés : {removed}")
    return df

        