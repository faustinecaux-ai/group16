"""Build STATS19 Silver without changing Bronze. Run with Spark 3.5 spark-submit."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import xml.etree.ElementTree as ET
import zipfile
from contextlib import redirect_stdout

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pipeline_common import create_spark, new_run_id, publish_run


KEYS = {
    "collisions": ["collision_index"],
    "vehicles": ["collision_index", "vehicle_reference"],
    "casualties": ["collision_index", "casualty_reference"],
}
CATEGORIES = {
    "collisions": ["collision_severity", "weather_conditions", "light_conditions",
                   "road_surface_conditions", "road_type"],
    "vehicles": ["vehicle_type"],
    "casualties": ["casualty_severity"],
}
IMPORTANT_FIELDS = {
    "collisions": ["collision_year", "date", "time", "collision_severity", "longitude",
                   "latitude", "speed_limit", "number_of_vehicles", "number_of_casualties"],
    "vehicles": ["collision_year", "vehicle_type", "age_of_driver"],
    "casualties": ["collision_year", "vehicle_reference", "casualty_severity", "age_of_casualty"],
}
AGE_FIELDS = {"vehicles": "age_of_driver", "casualties": "age_of_casualty"}
VALID_SPEED_LIMITS = [20, 30, 40, 50, 60, 70]  # Official guide: public highway limits, mph.


def read_source(spark, path):
    """
    Charge un fichier CSV STATS19 dans un DataFrame Spark.

    Spark déduit automatiquement les types des colonnes, mais certains
    champs comme les identifiants de collision et les heures sont
    explicitement conservés sous forme de chaînes de caractères.

    Cette précaution évite notamment de modifier les identifiants
    ou de perdre le format original des heures.
    """
    schema = spark.read.option("header", True).option("inferSchema", True).option(
        "mode", "FAILFAST").csv(str(path)).schema
    for field in schema:
        if field.name in ["collision_index", "collision_ref_no", "time"]:
            field.dataType = StringType()
    return spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(str(path))


def read_code_lists(path):
    """
    Extrait les correspondances entre les codes numériques et leurs
    descriptions depuis le guide officiel STATS19 au format XLSX.

    Le fichier Excel est lu directement comme une archive ZIP contenant
    des fichiers XML, sans utiliser de bibliothèque spécialisée Excel.

    La fonction récupère uniquement les champs utiles au projet
    et vérifie qu'un même code ne possède pas plusieurs descriptions
    contradictoires.

    Elle retourne un dictionnaire contenant les correspondances
    entre les codes et leurs libellés.
    """
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    selected = {field for fields in CATEGORIES.values() for field in fields}
    selected.add("enhanced_casualty_severity")
    selected.update(AGE_FIELDS.values())
    selected.add("speed_limit")
    with zipfile.ZipFile(path) as workbook:
        strings = ["".join(node.itertext()) for node in ET.fromstring(
            workbook.read("xl/sharedStrings.xml")).findall("s:si", ns)]
        result = {}
        rows = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        for row in rows.findall(".//s:sheetData/s:row", ns):
            values = {}
            for cell in row:
                node = cell.find("s:v", ns)
                value = node.text if node is not None else ""
                if cell.get("t") == "s":
                    value = strings[int(value)]
                column = "".join(c for c in cell.get("r") if c.isalpha())
                values[column] = value
            field, code, label = (values.get(c, "") for c in ["B", "C", "D"])
            if field in selected and code.lstrip("-").isdigit() and label:
                previous = result.setdefault(field, {}).get(int(code))
                if previous is not None and previous != label:
                    raise ValueError(f"Conflicting code list: {field}/{code}")
                result[field][int(code)] = label
    return result


def audit_keys(df, keys):
    """
    Vérifie la qualité des clés primaires d'un DataFrame.

    La fonction calcule :
    - le nombre total de lignes ;
    - le nombre de lignes ayant une clé manquante ou vide ;
    - le nombre de clés dupliquées en excès.

    Ces contrôles permettent de vérifier que chaque ligne peut être
    identifiée de manière unique dans son dataset.
    Renvoit une dataframe python
    """
    missing = F.lit(False)
    for key in keys:
        missing = missing | F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
    counts = df.agg(F.count("*").alias("rows"),
                    F.coalesce(F.sum(missing.cast("long")), F.lit(0)).alias("missing_keys")).first().asDict()
    counts["duplicate_key_excess"] = counts["rows"] - df.select(*keys).distinct().count()
    return counts


def decode(df, fields, mappings):
    """
    Ajoute des colonnes contenant les libellés associés aux codes
    numériques présents dans les données sources.

    Pour chaque champ catégoriel, la fonction utilise le dictionnaire
    de correspondance du guide officiel STATS19 pour créer une nouvelle
    colonne suffixée par "_label".

    Les colonnes contenant les codes d'origine sont conservées,
    ce qui permet de préserver les valeurs sources.
    Retourne la data frame 
    """
    for field in fields:
        mapping = mappings[field]
        pairs = [F.lit(value) for pair in mapping.items() for value in pair]
        df = df.withColumn(field + "_label", F.create_map(*pairs)[F.col(field)])
    return df


def audit_fields(df, fields):
    """
    Mesure le nombre de valeurs manquantes pour plusieurs colonnes.

    Une valeur est considérée comme manquante si elle est NULL
    ou si elle contient uniquement une chaîne vide ou des espaces.

    Les différents comptages sont réalisés dans une seule opération
    Spark afin de limiter le nombre de calculs distribués.

    retourne dico python avec le nombre pour chaque colone
    """
    return df.agg(*[
        F.sum((F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == ""))
              .cast("long")).alias(field) for field in fields
    ]).first().asDict()


## Que la colonne des âges
def prepare_age(df, field):
    """
    Nettoie les variables d'âge sans supprimer les valeurs sources.

    Les codes négatifs, notamment -1 qui représente une donnée
    manquante ou hors plage, sont remplacés par NULL dans une nouvelle
    colonne suffixée par "_clean".

    Les valeurs originales sont conservées dans leur colonne initiale.

    La fonction calcule également des indicateurs de qualité :
    codes manquants, valeurs négatives inattendues, valeurs NULL,
    âge minimum et âge maximum enregistrés.
    """
    age = F.col(field)
    clean = field + "_clean"
    df = df.withColumn(clean, F.when(age >= 0, age))
    metrics = df.agg(
        F.sum((age == -1).cast("long")).alias("source_missing_or_out_of_range_code"),
        F.sum((age < -1).cast("long")).alias("unexpected_negative_values"),
        F.sum(age.isNull().cast("long")).alias("source_nulls"),
        F.sum(F.col(clean).isNull().cast("long")).alias("clean_nulls"),
        F.min(clean).alias("minimum_recorded_age"),
        F.max(clean).alias("maximum_recorded_age"),
    ).first().asDict()
    return df, metrics


# Prépare le data frame des collisions
def prepare_collisions(df):
    """
    Prépare les variables temporelles et la limitation de vitesse
    du dataset collisions.

    La fonction :
    - conserve la date originale dans "date_source" ;
    - convertit la date au format date Spark ;
    - extrait l'heure uniquement si son format HH:mm est valide ;
    - crée une colonne contenant le mois ;
    - identifie les collisions survenues le week-end ;
    - crée une variable de limitation de vitesse nettoyée,
      en conservant uniquement les valeurs autorisées.

    Les colonnes originales restent disponibles pour les contrôles
    de qualité et la traçabilité.
    """
    return (df.withColumn("date_source", F.col("date"))
            .withColumn("date", F.to_date("date", "dd/MM/yyyy"))
            .withColumn("hour", F.when(F.col("time").rlike(r"^([01][0-9]|2[0-3]):[0-5][0-9]$"),
                                       F.substring("time", 1, 2).cast("int")))
            .withColumn("month", F.month("date"))
            .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
            .withColumn("speed_limit_mph_clean", F.when(F.col("speed_limit").isin(VALID_SPEED_LIMITS),
                                                       F.col("speed_limit"))))


def enrich_collisions(collisions, vehicles, casualties):
    """
    Enrichit le dataset collisions avec des informations agrégées
    provenant des datasets vehicles et casualties.

    Les véhicules sont d'abord regroupés par collision pour calculer
    leur nombre, le nombre d'âges de conducteurs connus et l'âge moyen.

    Les victimes sont également regroupées par collision pour calculer
    leur nombre total et les nombres de victimes fatales, graves
    et légères.

    Les deux tables agrégées sont ensuite jointes à collisions.

    Cette méthode évite une multiplication des lignes lors de la
    jointure entre les véhicules et les victimes et permet de conserver
    une seule ligne par collision.
    """
    vehicle_counts = vehicles.groupBy("collision_index").agg(
        F.count("*").alias("observed_vehicle_count"),
        F.count("age_of_driver_clean").alias("known_driver_age_count"),
        F.avg("age_of_driver_clean").alias("mean_recorded_driver_age"))
    casualty_counts = casualties.groupBy("collision_index").agg(
        F.count("*").alias("observed_casualty_count"),
        *[F.sum((F.col("casualty_severity") == code).cast("long")).alias(label + "_casualty_count")
          for code, label in [(1, "fatal"), (2, "serious"), (3, "slight")]])
    return collisions.join(vehicle_counts, "collision_index", "left").join(casualty_counts, "collision_index", "left")


def run(bronze, output, spark=None, run_id=None, bronze_report=None):
    """
    Exécute la construction et la validation de la couche Silver
    à partir des données de la couche Bronze.

    La fonction réalise les opérations suivantes :

    1. Initialise Spark et prépare le dossier de sortie de l'exécution.
    2. Vérifie que les données Bronze ont été validées.
    3. Charge les correspondances du guide officiel STATS19.
    4. Charge les trois datasets (collisions, vehicles, casualties).
    5. Vérifie les clés, les valeurs manquantes et les relations
       entre les données.
    6. Nettoie certaines variables, notamment les âges et les dates,
       tout en conservant les valeurs sources.
    7. Décode les variables catégorielles en ajoutant leurs libellés.
    8. Enrichit les collisions avec les informations agrégées
       des véhicules et des victimes.
    9. Vérifie la cohérence des données et leur conservation
       lors de l'écriture au format Parquet.
    10. Génère un rapport de qualité indiquant si l'exécution
        est validée (passed), nécessite une vérification (needs_review)
        ou a échoué (failed).

    Les anomalies détectées sont documentées plutôt que supprimées
    silencieusement. Les résultats sont publiés uniquement lorsque
    les contrôles requis sont satisfaits.

    Si la session Spark a été créée par cette fonction, elle est
    arrêtée à la fin de l'exécution. Les DataFrames mis en cache
    sont également libérés.
    """
    # Démarrage du chronomètre et préparation des chemins de sortie.
    started = time.monotonic()
    bronze, output = Path(bronze), Path(output)

    # Chaque exécution possède un identifiant et un dossier de sortie propres.
    run_id = run_id or new_run_id()
    destination = output / "runs" / run_id
    destination.mkdir(parents=True)

    # Rapport qui centralise les contrôles qualité de toute l'exécution.
    report = {
        "run_id": run_id,
        "status": "running",
        "tables": {},
        "relationships": {},
        "rejected_rows": 0,
        "policy": "No rows deleted; unknown source codes preserved."
    }

    owns_spark = spark is None
    frames = {}  # Contiendra les trois DataFrames Silver.
    cached = []  # Permettra de libérer les DataFrames mis en cache.

    try:
        # Création de la session Spark si elle n'a pas déjà été fournie.
        spark = spark or create_spark("STATS19 Silver")
        report["spark_version"] = spark.version

        # Silver ne doit pas utiliser des données Bronze dont la validation a échoué.
        if bronze_report is not None:
            if bronze_report["status"] != "passed":
                raise ValueError("Silver requires validated Bronze inputs")
            report["input_manifest"] = bronze_report

        # Lecture du guide officiel pour traduire les codes STATS19 en libellés.
        guide = bronze / "reference/stats19_data_guide.xlsx"
        mappings = read_code_lists(guide)

        # Empreinte SHA-256 du guide pour identifier précisément sa version.
        report["guide_sha256"] = hashlib.sha256(guide.read_bytes()).hexdigest()

        # Traitement successif des trois fichiers : collisions, véhicules et victimes.
        for name, keys in KEYS.items():
            print(f"Silver: checking {name}", flush=True)

            path = bronze / "raw" / name / f"{name}_last5years.csv"

            # Lecture du CSV avec Spark et mise en cache pour accélérer les traitements suivants.
            df = read_source(spark, path).cache()
            cached.append(df)

            # Contrôle des clés primaires et des valeurs manquantes.
            metrics = audit_keys(df, keys)
            metrics["input_schema"] = df.schema.jsonValue()
            metrics["missing_important_fields"] = audit_fields(
                df, IMPORTANT_FIELDS[name]
            )

            # Répartition des lignes par année.
            metrics["years"] = {
                str(r[0]): r[1]
                for r in df.groupBy("collision_year").count().collect()
            }
            report["tables"][name] = metrics

            # On bloque le traitement si le DataFrame est vide
            # ou si ses clés primaires sont manquantes ou dupliquées.
            if not metrics["rows"] or metrics["missing_keys"] or metrics["duplicate_key_excess"]:
                raise ValueError(f"{name}: missing or duplicate keys; publication blocked")

            # Nettoyage des colonnes d'âge dans les tables concernées.
            if name in AGE_FIELDS:
                field = AGE_FIELDS[name]
                df, metrics["age_quality"] = prepare_age(df, field)

                # Vérification de la règle officielle concernant le code d'âge -1.
                if mappings[field].get(-1) != "Data missing or out of range":
                    raise ValueError(f"Review official missing-age rule for {field}")

            # Traitements spécifiques au DataFrame des collisions.
            if name == "collisions":
                df = prepare_collisions(df)

                # Vérification de la règle officielle concernant les codes de vitesse.
                if mappings["speed_limit"].get(-1) != "Data missing or out of range":
                    raise ValueError("Review official speed-limit missing-code rule")

                # Mesure de la qualité des limitations de vitesse.
                metrics["speed_limit_quality"] = df.agg(
                    F.sum(F.col("speed_limit").isin(-1, 99).cast("long")).alias("coded_missing_or_unknown"),
                    F.sum((F.col("speed_limit").isNotNull() &
                           ~F.col("speed_limit").isin(VALID_SPEED_LIMITS + [-1, 99])).cast("long")).alias("unrecognised_values"),
                    F.sum(F.col("speed_limit_mph_clean").isNull().cast("long")).alias("clean_nulls")
                ).first().asDict()

                # Contrôles de cohérence des dates et des heures.
                metrics["date_parse_failures"] = df.filter(
                    F.col("date_source").isNotNull() & F.col("date").isNull()
                ).count()

                metrics["missing_or_unusable_time"] = df.filter(
                    F.col("hour").isNull()
                ).count()

                metrics["time_parse_failures"] = df.filter(
                    F.col("time").isNotNull() & (F.trim("time") != "") & F.col("hour").isNull()
                ).count()

                # Vérification de la cohérence entre la date et les informations calendaires.
                metrics["year_mismatches"] = df.filter(
                    F.year("date") != F.col("collision_year")
                ).count()

                metrics["weekday_mismatches"] = df.filter(
                    F.dayofweek("date") != F.col("day_of_week")
                ).count()

                # Vérification des plages géographiques autorisées.
                metrics["out_of_range_coordinates"] = df.filter(
                    (~F.col("latitude").between(-90, 90)) |
                    (~F.col("longitude").between(-180, 180))
                ).count()

            # Traduction des codes catégoriels en libellés lisibles.
            # Les colonnes contenant les codes originaux sont conservées.
            df = decode(df, CATEGORIES[name], mappings)

            # Recensement des codes qui n'ont pas de correspondance dans le guide.
            metrics["unmapped_categories"] = {}
            for field in CATEGORIES[name]:
                metrics["unmapped_categories"][field] = [
                    r.asDict()
                    for r in df.filter(
                        F.col(field + "_label").isNull()
                    ).groupBy(field).count().collect()
                ]

            # Enregistrement du nombre de lignes et du schéma après transformation.
            metrics["output_rows"] = df.count()
            metrics["output_schema"] = df.schema.jsonValue()

            # Stockage du DataFrame transformé.
            frames[name] = df

        # Récupération des trois DataFrames Silver.
        collisions, vehicles, casualties = [frames[n] for n in KEYS]



        #----------------------------------------------------------------

        # Vérification des relations entre les tables :
        # chaque véhicule et chaque victime doit correspondre à un accident existant.
        for name in ["vehicles", "casualties"]:
            report["relationships"][name + "_without_collision"] = frames[name].join(
                collisions.select("collision_index"), "collision_index", "left_anti"
            ).count()

            # Vérification de la cohérence des années entre les tables enfants et parents.
            report["relationships"][name + "_year_mismatches"] = frames[name].join(
                collisions.select(
                    "collision_index",
                    F.col("collision_year").alias("parent_year")
                ),
                "collision_index", "inner"
            ).filter(
                ~F.col("collision_year").eqNullSafe(F.col("parent_year"))
            ).count()

        # Vérification qu'une victime est bien associée à un véhicule existant.
        report["relationships"]["casualties_without_vehicle"] = casualties.join(
            vehicles.select("collision_index", "vehicle_reference"),
            ["collision_index", "vehicle_reference"], "left_anti"
        ).count()

        # Agrégation des informations véhicules et victimes par accident.
        # Le résultat conserve une seule ligne par collision.
        enriched = enrich_collisions(collisions, vehicles, casualties)

        # Sauvegarde du plan d'exécution Spark pour pouvoir examiner les jointures.
        with (destination / "enrichment_plan.txt").open("w") as handle, redirect_stdout(handle):
            enriched.explain(mode="formatted")

        # Comparaison des nombres déclarés dans collisions avec les nombres
        # réellement observés dans les tables vehicles et casualties.
        for field, source in [
            ("observed_vehicle_count", "number_of_vehicles"),
            ("observed_casualty_count", "number_of_casualties")
        ]:
            report["relationships"][field + "_mismatches"] = enriched.filter(
                F.coalesce(F.col(field), F.lit(0)) != F.col(source)
            ).count()

        # Vérification que l'enrichissement n'a pas créé ou supprimé de collisions.
        report["enriched_rows"] = enriched.count()
        if report["enriched_rows"] != report["tables"]["collisions"]["rows"]:
            raise ValueError("Collision grain changed")

        # Détermination du statut qualité :
        # une anomalie détectée peut nécessiter une revue sans forcément bloquer le traitement.
        needs_review = any(report["relationships"].values()) or any(
            any(m["unmapped_categories"].values()) for m in report["tables"].values()
        )

        needs_review = needs_review or any(
            report["tables"]["collisions"].get(k, 0) for k in
            ["date_parse_failures", "time_parse_failures", "year_mismatches",
             "weekday_mismatches", "out_of_range_coordinates"]
        )

        # Les valeurs d'âge négatives inattendues nécessitent une vérification.
        needs_review = needs_review or any(
            report["tables"][name]["age_quality"]["unexpected_negative_values"]
            for name in AGE_FIELDS
        )

        # Certaines informations essentielles ne devraient pas être manquantes.
        needs_review = needs_review or any(
            report["tables"]["collisions"]["missing_important_fields"][field]
            for field in [
                "date", "collision_year", "collision_severity",
                "number_of_vehicles", "number_of_casualties"
            ]
        )

        needs_review = needs_review or report["tables"]["collisions"][
            "speed_limit_quality"
        ]["unrecognised_values"]


        #-------------------------------------------------------------

        # Écriture des quatre DataFrames au format Parquet :
        # les trois tables détaillées et la table collisions enrichie.
        report["parquet_validation"] = {}
        for name, df in {**frames, "collisions_enriched": enriched}.items():
            print(f"Silver: writing {name}", flush=True)

            df.write.mode("errorifexists").parquet(str(destination / name))

            # On relit les fichiers Parquet pour vérifier que les données
            # écrites correspondent bien aux DataFrames d'origine.
            reloaded = spark.read.parquet(str(destination / name))

            schema_matches = [
                (f.name, f.dataType) for f in reloaded.schema
            ] == [
                (f.name, f.dataType) for f in df.schema
            ]

            count = reloaded.count()
            report["parquet_validation"][name] = {
                "rows": count,
                "schema_matches": schema_matches
            }

            if count != df.count() or not schema_matches:
                raise ValueError(f"Parquet validation failed: {name}")

        # Calcul de la taille totale des fichiers Parquet produits.
        report["output_bytes"] = sum(
            p.stat().st_size for p in destination.rglob("*.parquet")
        )

        # Statut final de l'exécution.
        report["status"] = "needs_review" if needs_review else "passed"


    #-------------------------------------------------------------------

    except Exception as error:
        # En cas d'erreur bloquante, l'exécution est marquée comme échouée.
        report["status"] = "failed"
        report["error"] = str(error)
        raise

    finally:
        # Ce bloc est exécuté même si une erreur survient.
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)

        # Libération de la mémoire utilisée par les DataFrames mis en cache.
        for df in cached:
            df.unpersist()

        # Arrêt de Spark uniquement si cette fonction a créé la session.
        if owns_spark and spark is not None:
            spark.stop()

        # Publication du rapport et mise à jour du pointeur
        # uniquement si le statut est passed.
        publish_run(output, destination, report)

        print(
            f"Silver status: {report['status']}; "
            f"report: {destination / 'quality_report.json'}",
            flush=True
        )

    return report

# Point d'entrée du script : permet de lancer la construction de Silver
# depuis le terminal en définissant les chemins des données Bronze
# et du répertoire de sortie.
#
# Le code initialise les arguments de ligne de commande, exécute
# la fonction run() et retourne un code de sortie différent selon
# que la validation de Silver a réussi ou non.
if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=root / "data/bronze/stats19")
    parser.add_argument("--output", type=Path, default=root / "data/silver")
    args = parser.parse_args()
    result = run(args.bronze.resolve(), args.output.resolve())
    raise SystemExit(0 if result["status"] == "passed" else 2)
