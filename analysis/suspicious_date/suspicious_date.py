import pandas as pd

clean_data_path = 'data/cleaned_data.json'
df = pd.read_json(clean_data_path, orient="records", lines=True)

df['suspicious_date'] = False

def mark_overlaps(group):
    count_suspicious_date = 0
    group = group.sort_values('start_time').copy()
    #init list suspicious
    suspicious = [False] * len(group)

    # boucle sur chaque doc
    for i in range(len(group)):
        current_start = group.iloc[i]['start_time']
        current_stop = group.iloc[i]['stop_time']

        # boucle sur chaque doc par group 
        for j in range(len(group)):
            # evite de comparer une période avec elle même
            if i != j:
                #recup start et stop time à la bonne position
                other_start = group.iloc[j]['start_time']
                other_stop = group.iloc[j]['stop_time']

                # check si chauvement
                if current_start < other_stop and current_stop > other_start:
                    count_suspicious_date += 1
                    suspicious[i] = True
                    break

    group['suspicious_date'] = suspicious
    
    return group

