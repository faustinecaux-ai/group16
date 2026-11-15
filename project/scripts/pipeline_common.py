"""Small shared helpers for durable reports and validated publication."""
import json
from pathlib import Path
import tempfile
from datetime import datetime, timezone


def new_run_id():
    """
    Génère un identifiant unique pour chaque exécution du pipeline.

    L'identifiant est basé sur la date et l'heure UTC, avec une
    précision à la microseconde. Cela permet de distinguer les
    différentes exécutions et de conserver leurs résultats séparément.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def write_json(path, value):
    """
    Enregistre des données dans un fichier JSON de manière atomique.

    Les données sont d'abord écrites dans un fichier temporaire.
    Une fois l'écriture terminée, ce fichier remplace le fichier
    de destination.

    Cela évite qu'un autre processus lise un fichier JSON incomplet
    si l'écriture est interrompue.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp",
                                     encoding="utf-8", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(value, handle, indent=2, allow_nan=False)
            handle.write("\n")
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    temporary.replace(path)


def publish_run(output, destination, report):
    """
    Publie les résultats d'une exécution du pipeline.

    Le rapport de qualité est d'abord enregistré dans le répertoire
    propre à cette exécution.

    Si le statut est "passed", le fichier latest_successful.json
    est ensuite mis à jour pour pointer vers cette exécution réussie.

    En cas d'échec, le rapport est conservé, mais le dernier
    traitement réussi reste inchangé.
    """
    destination = Path(destination)
    write_json(destination / "quality_report.json", report)
    if report["status"] != "passed":
        return False
    write_json(Path(output) / "latest_successful.json", {
        "run_id": report["run_id"], "path": str(destination.resolve())})
    return True


def load_validated_run(output):
    """
    Charge la dernière exécution réussie du pipeline.

    La fonction lit le fichier latest_successful.json pour retrouver
    le répertoire de la dernière exécution validée.

    Elle charge ensuite son rapport de qualité et vérifie que :
    - le statut est bien "passed" ;
    - l'identifiant de l'exécution correspond à celui du pointeur.

    Si ces vérifications échouent, une erreur est déclenchée.
    Cela empêche les étapes suivantes d'utiliser des résultats
    qui ne sont pas correctement validés.
    """
    pointer = json.loads((Path(output) / "latest_successful.json").read_text())
    destination = Path(pointer["path"])
    report = json.loads((destination / "quality_report.json").read_text())
    if report["status"] != "passed" or report["run_id"] != pointer["run_id"]:
        raise ValueError("Published run and quality report do not agree")
    return destination, report


def create_spark(name):
    """
    Initialise et configure une session Apache Spark.

    La session reçoit un nom d'application et plusieurs paramètres :
    - 8 partitions pour les opérations de shuffle ;
    - le fuseau horaire Europe/London, adapté aux données STATS19 ;
    - une politique de parsing des dates corrigée ;
    - la désactivation du mode ANSI SQL strict.

    Le niveau des logs Spark est réglé sur WARN pour limiter
    les messages affichés dans le terminal.
    """
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "Europe/London")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.ansi.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
