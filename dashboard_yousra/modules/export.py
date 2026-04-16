import pandas as pd
import io
import base64
from pymongo import MongoClient

def get_collection():
    client = MongoClient("mongodb://localhost:27018/")
    return client["telecom_algerie"]["dataset_unifie"]

def exporter_csv(limit=1000):
    """Exporte les commentaires en CSV"""
    col = get_collection()
    data = list(col.find({}, {"_id": 0}).limit(limit))
    df = pd.DataFrame(data)
    csv = df.to_csv(index=False).encode('utf-8')
    b64 = base64.b64encode(csv).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="commentaires.csv" class="download-link">📥 Télécharger CSV ({len(data)} commentaires)</a>'

def exporter_excel(limit=1000):
    """Exporte les commentaires en Excel"""
    col = get_collection()
    data = list(col.find({}, {"_id": 0}).limit(limit))
    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Commentaires')
    
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="commentaires.xlsx" class="download-link">📥 Télécharger Excel ({len(data)} commentaires)</a>'

def exporter_json(limit=1000):
    """Exporte les commentaires en JSON"""
    col = get_collection()
    data = list(col.find({}, {"_id": 0}).limit(limit))
    import json
    json_str = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    b64 = base64.b64encode(json_str.encode()).decode()
    return f'<a href="data:application/json;base64,{b64}" download="commentaires.json" class="download-link">📥 Télécharger JSON ({len(data)} commentaires)</a>'