import requests
import gzip
import io

def download_web_file(dbutils, url, target_folder, file_name):
    """
    Télécharge un fichier depuis une URL et l'écrit dans DBFS via dbutils.
    
    Args:
        dbutils: l'objet dbutils du notebook Databricks.
        url: URL du fichier à télécharger.
        target_folder: chemin DBFS de destination (ex: "/mnt/raw/airbnb").
        file_name: nom du fichier à créer.
        
    Supporte: .csv, .csv.gz, .json, .geojson
    """
    # Créer le dossier cible si nécessaire
    dbutils.fs.mkdirs(target_folder)
    
    # Téléchargement
    r = requests.get(url, stream=True)
    r.raise_for_status()
    
    lower_name = file_name.lower()
    
    # Traitement selon l'extension
    if lower_name.endswith(".csv.gz"):
        # Décompression gzip
        buf = io.BytesIO(r.content)
        with gzip.open(buf, "rt", encoding="utf-8") as f:
            content = f.read()
        # Ajouter _details avant .csv
        base_name = file_name[:-7]  # enlever .csv.gz
        target_path = f"{target_folder}/{base_name}_details.csv"
        
    elif lower_name.endswith(".csv"):
        content = r.text
        target_path = f"{target_folder}/{file_name}"
        
    elif lower_name.endswith(".json") or lower_name.endswith(".geojson"):
        content = r.text
        target_path = f"{target_folder}/{file_name}"
        
    else:
        raise ValueError(f"Extension non supportée : {file_name}")
    
    # Écriture dans DBFS
    dbutils.fs.put(target_path, content, overwrite=True)
    
    # Supprimer les fichiers cachés et .crc générés automatiquement
    for f in dbutils.fs.ls(target_folder):
        if f.name.startswith(".") or f.name.endswith(".crc"):
            dbutils.fs.rm(f.path, True)
    
    print(f"✅ Fichier téléchargé : {target_path}")
    return target_path

