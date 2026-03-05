import requests
import gzip
import io

def download_web_file_with_details(url, target_folder, file_name):
    """
    Télécharge un fichier depuis une URL et l'écrit dans DBFS.
    Supporte : .csv, .csv.gz, .json, .geojson
    Pour les fichiers .gz, ajoute '_details' avant l'extension finale.
    Supprime les fichiers .crc et cachés automatiquement.
    """
    # Créer le dossier cible
    dbutils.fs.mkdirs(target_folder)
    
    # Téléchargement
    r = requests.get(url, stream=True)
    r.raise_for_status()
    
    # Détection de l'extension
    lower_name = file_name.lower()
    if lower_name.endswith(".gz"):
        # décompression
        buf = io.BytesIO(r.content)
        with gzip.open(buf, "rt", encoding="utf-8") as f:
            content = f.read()
        # renommer : ajouter _details avant .csv
        base_name = file_name[:-7] if file_name.endswith(".csv.gz") else file_name[:-3]
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
    
    # Nettoyage fichiers cachés et .crc
    for f in dbutils.fs.ls(target_folder):
        if f.name.startswith(".") or f.name.endswith(".crc"):
            dbutils.fs.rm(f.path, True)
    
    print(f"✅ Fichier téléchargé : {target_path}")
    return target_path
