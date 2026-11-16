"""Publish reviewable evidence from the validated weather run; never invent metrics."""
import csv
import json
from pathlib import Path
import shutil

from pipeline_common import load_validated_run, write_json


def render(root):
    silver, report = load_validated_run(root / "data/silver/dev")
    gold, gold_report = load_validated_run(root / "data/gold/dev")
    if gold_report["silver_run_id"] != report["run_id"]:
        raise ValueError("Weather evidence requires matching Silver and Gold")
    metrics = report["weather_enrichment"]
    output = root / "reports"
    write_json(output / "weather_quality.json", dict(run_id=report["run_id"], **metrics))
    for name in ("weather_reconstructed", "weather_comparison"):
        shutil.copyfile(gold / (name + ".csv"), output / (name + ".csv"))
    text = f"""# Résultats de l'extension météo Silver

Exécution validée : `{report['run_id']}`. Bronze → Silver → Gold : passed.

| Indicateur | Résultat |
|---|---:|
| Accidents conservés (Grande-Bretagne, 2021–2025) | {metrics['rows']:,} |
| Dans la boîte londonienne, année 2023 | {metrics['in_scope']:,} |
| Météo reconstruite et appariée | {metrics['matched']:,} |
| Couverture du périmètre choisi | {metrics['coverage_in_scope_pct']} % |
| Couverture de l'ensemble national | {metrics['matched'] / metrics['rows'] * 100:.3f} % |
| Comparables avec un code policier 1–7 | {metrics['comparable']:,} |
| Accords avec le code policier | {metrics['agreements']:,} |
| Taux d'accord parmi les comparables | {metrics['agreement_pct']} % |
| Distance maximale observée au point météo | {metrics['max_distance_km']:.3f} km |
| Écart temporel maximal | {metrics['max_time_delta_minutes']} min |

## Statuts de rapprochement

"""
    text += "\n".join(f"- `{k}` : {v:,}" for k, v in sorted(metrics["status_counts"].items()))
    text += """

## Lecture des résultats

L'accord mesure la concordance entre deux descriptions différentes : un relevé policier local et une réanalyse horaire sur grille. Un désaccord ne désigne pas une erreur certaine de l'une des sources. Le seuil de vent de 40 km/h est une convention du projet. La couverture est limitée au périmètre londonien de 2023 et ne doit pas être présentée comme un enrichissement météo national complet.

Le travail Silver ajouté comprend acquisition traçable, normalisation JSON, contrôle des unités et clés, gestion des changements d'heure, rapprochement spatial et temporel, reconstruction indépendante, statuts d'exclusion et analyse de désaccord. Les totaux des deux nouvelles tables Gold se réconcilient avec tous les accidents, y compris ceux hors périmètre.

[Méthodologie](weather_methodology.md) · [Métriques et matrice de confusion](weather_quality.json) · [Comparaison Gold](weather_comparison.csv) · [Notebook](../notebooks/weather_extension.ipynb) · [Complément PDF](pdf/weather_extension.pdf)
"""
    (output / "weather_results.md").write_text(text)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    import numpy as np
    matrix = np.zeros((7, 8), dtype=int)
    for item in metrics["confusion_matrix"]:
        matrix[item["weather_conditions"] - 1, item["weather_conditions_reconstructed"] - 1] = item["count"]
    fig, ax = plt.subplots(figsize=(10, 6))
    im = ax.imshow(np.log1p(matrix), cmap="Blues")
    for (i, j), value in np.ndenumerate(matrix):
        ax.text(j, i, str(value), ha="center", va="center", fontsize=9,
                color="white" if np.log1p(value) > im.get_clim()[1] * .6 else "black")
    ax.set(xticks=range(8), xticklabels=range(1, 9), yticks=range(7), yticklabels=range(1, 8),
           xlabel="Reconstructed STATS19-like code", ylabel="Police STATS19 code",
           title=f"Independent weather reconstruction — London box, 2023\nComparable collisions: {metrics['comparable']:,}; agreement: {metrics['agreement_pct']}%")
    fig.colorbar(im, ax=ax, label="log(1 + collision count)")
    fig.tight_layout()
    fig.savefig(output / "weather_confusion.png", dpi=160)
    with PdfPages(output / "pdf/weather_extension.pdf") as pdf:
        cover = plt.figure(figsize=(8.27, 11.69))
        cover.text(.09, .92, "DAT535 — Silver weather extension", fontsize=19)
        paragraphs = [
            "Faustine Caux and Jules Gombert — Group 16",
            "Independent source: ECMWF/Copernicus ERA5 via Open-Meteo.\nHourly reanalysis; 0.25 degree request grid. CC BY 4.0.",
            "Executed scope: London bounding box, full year 2023.\nLatitude 51.25–51.75; longitude -0.50–0.25.\nAll national collisions remain in the pipeline.",
            f"National collisions retained: {metrics['rows']:,}\nIn scope: {metrics['in_scope']:,}; matched: {metrics['matched']:,}\nLocal coverage: {metrics['coverage_in_scope_pct']}%\nComparable police codes 1–7: {metrics['comparable']:,}\nAgreement: {metrics['agreement_pct']}%",
            "Silver engineering added:\nImmutable JSON snapshots and SHA-256 provenance.\nArray normalization, units and unique-key checks.\nLondon wall time to UTC, explicit DST exclusions.\nNearest-hour join, at most 30 minutes.\nHaversine distance check using returned API coordinates.\nIndependent WMO/wind classification; police values preserved.\nCoverage, exclusions, confusion matrix and grain checks.",
            "Limits: regional coverage, gridded hourly estimates, local phenomena.\nWind threshold 40 km/h is a project convention.\nDisagreement does not establish police or model error.\nRetrospective matching can use a later hourly observation.\nPrecipitation is a preceding-hour sum, not minute-level rainfall.",
            f"Validated run: {report['run_id']}\nSee weather_methodology.md and weather_quality.json for full evidence.",
        ]
        y = .86
        for paragraph in paragraphs:
            cover.text(.09, y, paragraph, fontsize=10, va="top", linespacing=1.5)
            y -= .022 * (paragraph.count("\n") + 1) + .020
        pdf.savefig(cover)
        pdf.savefig(fig)
        plt.close(cover)
    plt.close(fig)

    # A small runnable notebook, separate from the historical baseline notebook.
    cells = [
        {"cell_type": "markdown", "metadata": {}, "source": ["# Extension Silver : météo indépendante\n", "ERA5 via Open-Meteo, boîte londonienne en 2023. Voir `reports/weather_methodology.md`. Les autres accidents restent hors périmètre.\n"]},
        {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": [
            "from pathlib import Path\n", "import json\n", "import pandas as pd\n",
            "root = Path.cwd() if (Path.cwd() / 'reports').exists() else Path.cwd().parent\n",
            "metrics = json.loads((root / 'reports/weather_quality.json').read_text())\n",
            "pd.Series({k: v for k, v in metrics.items() if k not in ('source_manifest', 'confusion_matrix')})\n"]},
        {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": [
            "confusion = pd.DataFrame(metrics['confusion_matrix'])\n",
            "confusion.pivot(index='weather_conditions', columns='weather_conditions_reconstructed', values='count').fillna(0).astype(int)\n"]},
        {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": [
            "from IPython.display import Image, display\n", "display(Image(filename=str(root / 'reports/weather_confusion.png')))\n"]},
        {"cell_type": "markdown", "metadata": {}, "source": ["Le taux d'accord exclut les codes policiers inconnus/autres. La réanalyse ne représente pas une vérité terrain à chaque accident. Le seuil de vent est une hypothèse de projet ; les désaccords et les exclusions sont des résultats de qualité à documenter.\n"]},
    ]
    write_json(root / "notebooks/weather_extension.ipynb", {"cells": cells, "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}}, "nbformat": 4, "nbformat_minor": 4})
    print(text)


if __name__ == "__main__":
    render(Path(__file__).resolve().parents[1])
