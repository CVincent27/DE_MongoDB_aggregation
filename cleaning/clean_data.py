def clean_keys(doc):
    if isinstance(doc, dict):
        return {
            k.replace(' ', '_') if isinstance(k, str) else k: clean_keys(v)
            for k, v in doc.items()
        }
    elif isinstance(doc, list):
        return [clean_keys(item) for item in doc]
    else:
        return doc
    
