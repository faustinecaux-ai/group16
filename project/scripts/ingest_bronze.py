"""Validate a manually acquired STATS19 snapshot without changing source files."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import time
from pipeline_common import new_run_id, publish_run

FILES = {name: f"raw/{name}/{name}_last5years.csv"
         for name in ["collisions", "vehicles", "casualties"]}
SOURCE_URL = "https://www.gov.uk/government/statistical-data-sets/road-safety-open-data"


def compute_sha256(file_path):
    """
    Calcule l'empreinte numérique SHA-256 d'un fichier.

    Le fichier est lu par blocs de 1 Mo afin d'éviter de le charger
    entièrement en mémoire. Cette empreinte permet de détecter si
    le contenu du fichier a changé entre deux exécutions du pipeline.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def read_csv_header(file_path):
    """
    Lit uniquement la première ligne d'un fichier CSV pour récupérer
    les noms de ses colonnes.

    Cette vérification permet de contrôler la structure du fichier
    et de s'assurer que les colonnes essentielles sont présentes,
    sans avoir besoin de charger toutes les données.
    """
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def run(bronze, output, run_id=None):
    """
    Exécute l'étape d'ingestion et de validation de la couche Bronze.

    Pour chacun des trois fichiers STATS19 (collisions, vehicles
    et casualties), le script vérifie :
    - que le fichier existe et n'est pas vide ;
    - que ses colonnes ne sont pas dupliquées ;
    - que les colonnes obligatoires sont présentes ;
    - que son empreinte SHA-256 est calculée pour détecter
      les éventuels changements depuis la précédente exécution.

    Le script vérifie également la présence du guide officiel STATS19.

    Enfin, il génère un rapport de qualité indiquant si la validation
    a réussi (passed) ou échoué (failed), sans modifier les fichiers
    sources.
    """
    bronze, output = Path(bronze), Path(output)

    # Génère un nouvel identifiant si aucun n'a été fourni.
    run_id = run_id or new_run_id()

    # Chaque exécution possède son propre dossier pour conserver son rapport.
    destination = output / "runs" / run_id
    destination.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()

    # Initialise le rapport qui sera complété au fur et à mesure des vérifications.
    report = {
        "run_id": run_id,
        "status": "running",
        "source_url": SOURCE_URL,
        "datasets": [],
        "format": "CSV",
        "raw_files_modified": False
    }

    try:
        # Récupère les empreintes SHA-256 de la dernière exécution réussie
        # afin de pouvoir détecter les changements dans les fichiers sources.
        pointer = output / "latest_successful.json"
        if pointer.exists():
            previous_path = Path(json.loads(pointer.read_text())["path"]) / "quality_report.json"
        else:
            previous_path = bronze / "metadata/ingestion_metadata.json"

        previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
        hashes = {
            item["name"]: item.get("sha256")
            for item in previous.get("datasets", [])
        }

        # Parcourt les trois datasets : collisions, vehicles et casualties.
        for name, relative in FILES.items():
            path = bronze / relative

            # Vérifie que le fichier existe et qu'il n'est pas vide.
            if not path.is_file():
                raise FileNotFoundError(f"Missing {name} source: {path}")
            if not path.stat().st_size:
                raise ValueError(f"Empty source: {path}")

            # Vérifie la structure du CSV : absence de doublons
            # et présence des colonnes indispensables à chaque dataset.
            columns = read_csv_header(path)
            if len(columns) != len(set(columns)):
                raise ValueError(f"Duplicate column names: {path}")

            required = {"collision_index", "collision_year"}
            if name != "collisions":
                required.add("vehicle_reference")
            if name == "casualties":
                required.add("casualty_reference")

            if not required.issubset(columns):
                raise ValueError(f"Missing source columns in {name}: {required - set(columns)}")

            # Calcule l'empreinte SHA-256 et la compare à celle
            # de la précédente exécution pour détecter les changements.
            checksum = compute_sha256(path)

            # Ajoute les informations du fichier au rapport de qualité.
            report["datasets"].append({
                "name": name,
                "file": relative,
                "size_bytes": path.stat().st_size,
                "sha256": checksum,
                "columns": columns,
                "column_count": len(columns),
                "change_status": "unchanged" if hashes.get(name) == checksum else "new_or_changed"
            })

            print(f"Bronze: {name} checked ({path.stat().st_size:,} bytes)", flush=True)

        # Vérifie également la présence du guide officiel STATS19
        # et conserve son empreinte pour assurer la traçabilité.
        guide = bronze / "reference/stats19_data_guide.xlsx"
        if not guide.is_file() or not guide.stat().st_size:
            raise ValueError(f"Missing or empty reference guide: {guide}")

        report["reference"] = {
            "file": "reference/stats19_data_guide.xlsx",
            "sha256": compute_sha256(guide),
            "size_bytes": guide.stat().st_size
        }

        # Toutes les vérifications ont réussi.
        report["status"] = "passed"

    except Exception as error:
        # En cas d'erreur, marque l'exécution comme échouée
        # et conserve le message d'erreur dans le rapport.
        report["status"] = "failed"
        report["error"] = str(error)
        raise

    finally:
        # Ce bloc est exécuté même si une erreur survient :
        # on mesure la durée et on conserve le rapport de l'exécution.
        # Le pointeur latest_successful ne sera mis à jour que si le statut
        # est "passed" (c'est géré par publish_run).
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)
        publish_run(output, destination, report)

    return report


# Point d'entrée du script : permet de lancer l'ingestion depuis
# le terminal en définissant les chemins des données Bronze et
# du répertoire de sortie à l'aide d'arguments.
if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=root / "data/bronze/stats19")
    parser.add_argument("--output", type=Path, default=root / "data/pipeline/dev/bronze")
    args = parser.parse_args()
    result = run(args.bronze, args.output)
    print(f"Bronze status: {result['status']}; run: {result['run_id']}")
