import csv
import json
import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"
ETUDIANTS_RAW = f"{DATA_DIR}/etudiants_raw.csv"
ETUDIANTS_CLEAN = f"{DATA_DIR}/etudiants_clean.csv"
RESULT_GROUPES = f"{DATA_DIR}/affectation_groupes.json"
RESULT_STATS = f"{DATA_DIR}/statistiques_inscriptions.json"
FINAL_REPORT = f"{DATA_DIR}/rapport_inscription.txt"

def reception_fichier():
    os.makedirs(DATA_DIR, exist_ok=True)
    # Simulate receiving the raw student csv file
    etudiants = [
        ["id_etudiant", "nom", "age", "ville", "filiere", "note"],
        [101, "Ali", 20, "Casablanca", "BDDC", 14.5],
        [102, "Sanaa", 21, "Rabat", "BDDC", 9.0],
        [103, "Omar", 22, "Marrakech", "BDDC", 16.0],
        [104, "Yasmine", 19, "Casablanca", "BDDC", 11.5],
        [105, "Hamza", 20, "Tanger", "BDDC", 8.5],
        [106, "Layla", 21, "Rabat", "BDDC", 15.0],
    ]
    with open(ETUDIANTS_RAW, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(etudiants)
    print(f"[RECEPTION] Raw file created : {ETUDIANTS_RAW}")

def stockage_zone_brute():
    if not os.path.exists(ETUDIANTS_RAW):
        raise FileNotFoundError("The raw student file does not exist.")
    taille = os.path.getsize(ETUDIANTS_RAW)
    print(f"[STOCKAGE] Saved raw file in Raw zone (Size: {taille} bytes).")

def validation_fichier():
    if not os.path.exists(ETUDIANTS_RAW):
        raise FileNotFoundError("The raw student file is missing.")
    with open(ETUDIANTS_RAW, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)
    colonnes_attendues = ["id_etudiant", "nom", "age", "ville", "filiere", "note"]
    if header != colonnes_attendues:
        raise ValueError("Incorrect headers for student file schema.")
    print(f"[VALIDATION] Schema validation successful. Columns: {header}")

def nettoyage_donnees():
    lignes_propres = []
    with open(ETUDIANTS_RAW, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            note = float(row["note"])
            # Add calculated decision field
            decision = "Admis" if note >= 10 else "Ajourne"
            lignes_propres.append({
                "id_etudiant": int(row["id_etudiant"]),
                "nom": row["nom"],
                "age": int(row["age"]),
                "ville": row["ville"],
                "filiere": row["filiere"],
                "note": note,
                "decision": decision
            })
    with open(ETUDIANTS_CLEAN, mode="w", newline="", encoding="utf-8") as file:
        fieldnames = ["id_etudiant", "nom", "age", "ville", "filiere", "note", "decision"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(lignes_propres)
    print(f"[NETTOYAGE] Cleaned file generated : {ETUDIANTS_CLEAN}")

def affectation_groupes():
    # Allocate students into 2 groups (Groupe_A and Groupe_B) alternatively
    groupes = {"Groupe_A": [], "Groupe_B": []}
    with open(ETUDIANTS_CLEAN, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader):
            if idx % 2 == 0:
                groupes["Groupe_A"].append(row["nom"])
            else:
                groupes["Groupe_B"].append(row["nom"])
    with open(RESULT_GROUPES, mode="w", encoding="utf-8") as file:
        json.dump(groupes, file, indent=4, ensure_ascii=False)
    print(f"[AFFECTATION] Students allocated to groups.")

def generation_statistiques():
    notes = []
    admis_count = 0
    total_count = 0
    with open(ETUDIANTS_CLEAN, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            note = float(row["note"])
            notes.append(note)
            total_count += 1
            if row["decision"] == "Admis":
                admis_count += 1
    stats = {
        "nombre_total": total_count,
        "note_moyenne": sum(notes) / len(notes) if notes else 0,
        "note_max": max(notes) if notes else 0,
        "note_min": min(notes) if notes else 0,
        "taux_reussite_pourcent": (admis_count / total_count * 100) if total_count else 0
    }
    with open(RESULT_STATS, mode="w", encoding="utf-8") as file:
        json.dump(stats, file, indent=4, ensure_ascii=False)
    print(f"[STATISTIQUES] Computed statistics: {stats}")

def rapport_final():
    with open(RESULT_GROUPES, mode="r", encoding="utf-8") as file:
        groupes = json.load(file)
    with open(RESULT_STATS, mode="r", encoding="utf-8") as file:
        stats = json.load(file)
    with open(FINAL_REPORT, mode="w", encoding="utf-8") as report:
        report.write("RAPPORT DES INSCRIPTIONS ETUDIANTS\n")
        report.write("==================================\n\n")
        report.write(f"Nombre d'etudiants inscrits : {stats['nombre_total']}\n")
        report.write(f"Note Moyenne : {stats['note_moyenne']:.2f}/20\n")
        report.write(f"Note Maximale : {stats['note_max']}/20 | Note Minimale : {stats['note_min']}/20\n")
        report.write(f"Taux de reussite : {stats['taux_reussite_pourcent']:.1f}%\n\n")
        report.write("Repartition des groupes :\n")
        for groupe, liste in groupes.items():
            report.write(f"  - {groupe} : {', '.join(liste)}\n")
    print(f"[RAPPORT] Final report generated: {FINAL_REPORT}")

with DAG(
    dag_id="pipeline_inscription_etudiants",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-projet", "python-operator"],
) as dag:

    reception = PythonOperator(task_id="reception_fichier", python_callable=reception_fichier)
    stockage = PythonOperator(task_id="stockage_zone_brute", python_callable=stockage_zone_brute)
    validation = PythonOperator(task_id="validation_fichier", python_callable=validation_fichier)
    nettoyage = PythonOperator(task_id="nettoyage_donnees", python_callable=nettoyage_donnees)
    affectation = PythonOperator(task_id="affectation_groupes", python_callable=affectation_groupes)
    statistiques = PythonOperator(task_id="generation_statistiques", python_callable=generation_statistiques)
    rapport = PythonOperator(task_id="rapport_final", python_callable=rapport_final)

    # Define parallel execution structure
    reception >> stockage >> validation >> nettoyage
    nettoyage >> [affectation, statistiques]
    [affectation, statistiques] >> rapport