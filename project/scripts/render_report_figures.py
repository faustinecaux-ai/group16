"""Render report figures from versioned evidence; no raw CSVs or Spark required."""
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


ROOT = Path(__file__).resolve().parents[1]
BLUE = "#195369"
ORANGE = "#b65b32"


def run():
    reports = ROOT / "reports"
    quality = json.loads((reports / "silver_quality.json").read_text())
    results = json.loads((reports / "results.json").read_text())
    if quality["status"] != "passed" or quality["run_id"] != results["silver_run_id"]:
        raise ValueError("Report figures require matching, passed Silver evidence")
    figures = reports / "figures"
    figures.mkdir(exist_ok=True)
    plt.rcParams.update({"font.size": 12, "pdf.fonttype": 42})

    # Make the two grain reductions explicit before collision-level enrichment.
    fig, ax = plt.subplots(figsize=(11.5, 3.3), layout="constrained")
    ax.set(xlim=(0, 11.5), ylim=(0, 3.3))
    ax.axis("off")

    def box(x, y, w, h, label, color=BLUE):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08",
                                   edgecolor=color, facecolor="#f3f7f8", linewidth=1.2))
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", fontsize=13)

    def arrow(start, end):
        ax.annotate("", xy=end, xytext=start,
                    arrowprops={"arrowstyle": "->", "color": BLUE, "lw": 1.5})

    box(.15, 2.25, 3.0, .7, "Vehicles\n1 row per collision + vehicle")
    box(.15, .35, 3.0, .7, "Casualties\n1 row per collision + casualty")
    box(4.0, 2.25, 2.6, .7, "Vehicle summary\n1 row per collision")
    box(4.0, .35, 2.6, .7, "Casualty summary\n1 row per collision")
    box(7.8, 2.25, 3.3, .7, "Collisions\n1 row per collision")
    box(7.8, .35, 3.3, .7, "Collisions enriched\n513,801 rows retained", ORANGE)
    arrow((3.25, 2.6), (3.9, 2.6))
    arrow((3.25, .7), (3.9, .7))
    arrow((6.7, 2.6), (7.7, .85))
    arrow((6.7, .7), (7.7, .7))
    arrow((9.45, 2.15), (9.45, 1.15))
    ax.text(3.6, 1.65, "Aggregate children independently", ha="center", color=BLUE, fontsize=12)
    ax.text(9.45, 1.65, "Left joins", ha="center", color=BLUE, fontsize=12)
    fig.savefig(figures / "silver_model.pdf", bbox_inches="tight")
    plt.close(fig)

    # A populated -1 is missing information, even when a null-only audit is zero.
    fig, ax = plt.subplots(figsize=(10.5, 2.6), layout="constrained")
    names = ["vehicles", "casualties"]
    missing = [100 * quality["tables"][n]["age_quality"]["clean_nulls"] /
               quality["tables"][n]["rows"] for n in names]
    labels = ["Driver age (vehicle records)", "Casualty age (casualty records)"]
    for i, (n, pct) in enumerate(zip(names, missing)):
        t = quality["tables"][n]
        ax.barh(i, 100 - pct, color=BLUE, height=.48,
                label="Recorded age retained" if i == 0 else None)
        ax.barh(i, pct, left=100 - pct, color=ORANGE, height=.48,
                label="Missing code → null in clean column" if i == 0 else None)
        ax.text(2, i, f"{t['rows']:,} rows retained; {t['age_quality']['clean_nulls']:,} missing ages ({pct:.2f}%)",
                color="white", va="center", fontsize=12)
    ax.set(yticks=[0, 1], yticklabels=labels, xlim=(0, 100),
           xlabel="Share of records (%)")
    ax.invert_yaxis()
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.legend(loc="upper center", bbox_to_anchor=(.5, -.35), ncol=2, frameon=False, fontsize=11)
    fig.savefig(figures / "silver_age_quality.pdf", bbox_inches="tight")
    plt.close(fig)

    # Show volume and within-profile severity on separate scales with explicit units.
    selected = results["top_profiles"][:6]
    labels = [r["profile"].replace(" / Single carriageway / ", " / ") for r in selected]
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 3.5), sharey=True, layout="constrained")
    positions = list(range(len(selected)))
    counts = [r["fatal_or_serious_collision_count"] for r in selected]
    percentages = [r["fatal_or_serious_percentage"] for r in selected]
    axes[0].barh(positions, counts, color=BLUE, height=.6)
    axes[0].set(yticks=positions, yticklabels=labels, xlim=(0, max(counts) * 1.24),
                xlabel="Fatal or serious collisions (count)")
    axes[1].barh(positions, percentages, color=ORANGE, height=.6)
    axes[1].set(xlim=(0, 46), xlabel="Fatal or serious within profile (%)")
    axes[1].axvline(results["fatal_or_serious_percentage"], color=BLUE, linestyle="--",
                    label=f"Overall: {results['fatal_or_serious_percentage']:.3f}%")
    axes[0].invert_yaxis()
    for i, (count, pct) in enumerate(zip(counts, percentages)):
        axes[0].text(count + max(counts) * .02, i, f"{count:,}", va="center", fontsize=13)
        axes[1].text(pct + .6, i, f"{pct:.3f}", va="center", fontsize=13)
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="x", alpha=.15)
        ax.set_axisbelow(True)
    fig.suptitle("Six profiles with the most severe cases — all on single carriageways, 2021–2025", fontsize=11)
    fig.savefig(figures / "report_profiles.pdf", bbox_inches="tight")
    plt.close(fig)
    with (reports / "profile_years.csv").open(newline="") as handle:
        annual = list(csv.DictReader(handle))
    fig, ax = plt.subplots(figsize=(9.5, 3.5), layout="constrained")
    for profile in results["top_profiles"][:5]:
        rows = sorted([r for r in annual if
                       r["light_conditions_label"] == profile["light_conditions_label"] and
                       r["road_type_label"] == profile["road_type_label"] and
                       float(r["speed_limit_mph_clean"]) == profile["speed_limit_mph_clean"]],
                      key=lambda r: int(r["collision_year"]))
        years = [int(r["collision_year"]) for r in rows]
        if years != results["annual_profile_checks"][0]["expected_years"]:
            raise ValueError("Annual report figure has incomplete coverage")
        values = [float(r["share_of_all_fatal_or_serious_percentage"]) for r in rows]
        label = profile["profile"].replace(" / Single carriageway / ", " / ")
        ax.plot(years, values, marker="o", label=label)
    ax.set(xlabel="Year", ylabel="Share of severe collisions in year (%)",
           xticks=years, ylim=(0, 34))
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(alpha=.2)
    ax.legend(loc="upper center", bbox_to_anchor=(.5, -.3), ncol=2, frameon=False, fontsize=11)
    fig.savefig(figures / "report_profile_years.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"Four report figures written to {figures}")


if __name__ == "__main__":
    run()
