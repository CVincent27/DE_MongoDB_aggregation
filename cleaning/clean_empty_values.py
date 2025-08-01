def replace_empty_strings(doc):
    '''remplace "" par None dans les dicts/lists'''
    if isinstance(doc, dict):
        return {k: replace_empty_strings(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [replace_empty_strings(item) for item in doc]
    elif doc == "":
        return None
    else:
        return doc