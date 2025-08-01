import pandas as pd
import datetime as dt

def check_gender(df):
    one_gender = df['gender'] == 1
    two_gender = df['gender'] == 2
    zero_gender = df['gender'] ==0

    count_one_gender = one_gender.sum()
    count_two_gender = two_gender.sum()
    count_zero_gender = zero_gender.sum()

    print(f"\nNb gender 1: {count_one_gender}")
    print(f"Nb gender 2: {count_two_gender}")
    print(f"Nb gender 0: {count_zero_gender}")
    return df