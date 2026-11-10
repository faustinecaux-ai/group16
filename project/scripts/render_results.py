"""Build reproducible figures and a short results page from small Gold CSV tables."""
import argparse
import csv
import json
from pathlib import Path

from pipeline_common import load_validated_run, write_json


def profile_label(row):
    value = row["speed_limit_mph_clean"]
    speed = "Unknown speed" if value is None or str(value) in ["", "nan"] else f"{float(value):g} mph"
    return f"{row['light_conditions_label']} / {row['road_type_label']} / {speed}"



def analytical_answer(summary):
    """State the answer and its annual evidence using the validated run's measures."""
    leading = summary["top_profiles"][0]
    checks = summary["annual_profile_checks"]
    lines = [
        "## Answer to the research question", "",
        f"**{leading['profile']} accounts for the largest number of fatal or serious reported "
        f"collisions: {leading['fatal_or_serious_collision_count']:,}, or "
        f"{leading['share_of_all_fatal_or_serious_percentage']:.3f}% of all severe collisions.** "
        f"The five leading profiles together contain {summary['top_five_severe_collision_count']:,} "
        f"severe collisions ({summary['top_five_share_of_all_severe_percentage']:.3f}%).", "",
        f"The leading profile's within-profile severe proportion is "
        f"{leading['fatal_or_serious_percentage']:.3f}%, compared with "
        f"{summary['fatal_or_serious_percentage']:.3f}% across the snapshot. "
        f"Its share of all recorded collisions is {leading['share_of_all_collisions_percentage']:.3f}%. "
        "Its large severe caseload therefore accompanies a large recorded collision population; "
        "it does not establish a higher probability of a severe outcome than the overall recorded-collision reference.", "",
        "### Does the pooled ranking hold each year?", "",
        "The following profiles were selected from the pooled period, then followed in each year. "
        "Ranks use severe-case counts, with collision counts and profile codes breaking ties. "
        "The share gap is the share of severe collisions minus the share of all recorded collisions, "
        "calculated against each year's totals.", "",
        "| Pooled leading profile | Years observed | Annual rank range | Years with positive / negative share gap |",
        "|---|---|---:|---:|",
    ]
    for check in checks[:5]:
        years = ", ".join(str(year) for year in check["years_observed"])
        lines.append(f"| {check['profile']} | {years} | {check['rank_min']}–{check['rank_max']} | "
                     f"{check['positive_gap_years']} / {check['negative_gap_years']} |")
    stable = all(c["years_observed"] == c["expected_years"] and
                 c["rank_min"] == c["rank_max"] == rank
                 for rank, c in enumerate(checks[:5], 1)) and len(checks) >= 5
    lines.extend(["", (
        "**The same five profiles occupy ranks 1–5 in the same order in every year from "
        + str(checks[0]["expected_years"][0]) + " to " + str(checks[0]["expected_years"][-1]) + ".** "
        if stable else "Annual ranks and coverage are shown above; the pooled ranking alone does not establish annual stability. "
    ) + "This supports the descriptive consistency of the pooled result within the observed period. "
        "It is not independent validation or evidence that reporting changes have no effect.", "",
        "### What should an analyst do with this result?", "",
        "Use the largest severe-case groups to select where to investigate the recorded burden, "
        "and inspect within-profile severity alongside counts to identify different analytical priorities. "
        "Next, disaggregate these groups by location and year, review recording and road context, "
        "and obtain traffic volumes or distance travelled before comparing exposure-adjusted risk "
        "or choosing an intervention. The lighting, road-type and speed-limit associations cannot "
        "identify the effect of changing any one of those conditions.", "",
    ])
    return "\n".join(lines)


def run(gold, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    path, report = load_validated_run(gold)
    output = Path(output)
    figures = output / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    profiles = pd.read_csv(path / "collision_profiles.csv").sort_values(
        ["fatal_or_serious_collision_count", "collision_count", "light_conditions", "road_type", "speed_limit_mph_clean"],
        ascending=[False, False, True, True, True])
    top_profiles = profiles.head(10).copy()
    top_profiles["profile"] = top_profiles.apply(profile_label, axis=1)
    chart = top_profiles.iloc[::-1]
    fig, axes = plt.subplots(1, 2, figsize=(16, 9), sharey=True, layout="constrained")
    positions = list(range(len(chart)))
    axes[0].barh(positions, chart.fatal_or_serious_collision_count, color="#23708d")
    axes[0].set_yticks(positions, chart.profile.str.replace(" / ", "\n", n=1))
    axes[0].set(xlabel="Fatal or serious collisions (count)")
    axes[1].barh(positions, chart.fatal_or_serious_percentage, color="#ae4d36")
    axes[1].set(xlabel="Fatal or serious / reported collisions in profile (%)")
    for axis in axes:
        axis.grid(axis="x", alpha=0.2)
    fig.suptitle("Ten profiles with the most fatal or serious reported collisions, 2021–2025")
    fig.savefig(figures / "collision_profiles.png", dpi=150)
    plt.close(fig)
    fig, axis = plt.subplots(figsize=(12, 8), layout="constrained")
    axis.barh([v - 0.18 for v in positions], chart.share_of_all_collisions_percentage,
              height=0.35, color="#87959c", label="Share of all reported collisions")
    axis.barh([v + 0.18 for v in positions], chart.share_of_all_fatal_or_serious_percentage,
              height=0.35, color="#ae4d36", label="Share of all fatal or serious collisions")
    axis.set_yticks(positions, chart.profile.str.replace(" / ", "\n", n=1))
    axis.set(xlabel="Share of the corresponding recorded collision population (%)",
             title="Profile frequency compared with severe-case concentration")
    axis.legend(loc="lower right", fontsize=9)
    axis.grid(axis="x", alpha=0.2)
    fig.savefig(figures / "profile_reference.png", dpi=150)
    plt.close(fig)
    annual = pd.read_csv(path / "collision_profiles_by_year.csv").sort_values(
        ["collision_year", "fatal_or_serious_collision_count", "collision_count", "light_conditions", "road_type", "speed_limit_mph_clean"],
        ascending=[True, False, False, True, True, True])
    annual["profile"] = annual.apply(profile_label, axis=1)
    annual["severe_count_rank"] = annual.groupby("collision_year").cumcount() + 1
    expected_years = sorted(int(year) for year in report["source_year_totals"])
    annual_checks = []
    for label in top_profiles.profile:
        rows = annual[annual.profile == label]
        gap = rows.severe_minus_all_collision_share_pp
        annual_checks.append({
            "profile": label, "years_observed": sorted(int(y) for y in rows.collision_year),
            "expected_years": expected_years,
            "rank_min": int(rows.severe_count_rank.min()) if len(rows) else None,
            "rank_max": int(rows.severe_count_rank.max()) if len(rows) else None,
            "positive_gap_years": int((gap > 0).sum()),
            "negative_gap_years": int((gap < 0).sum()),
            "zero_gap_years": int((gap == 0).sum()),
            "undefined_gap_years": int(gap.isna().sum())})
    fig, axis = plt.subplots(figsize=(12, 6), layout="constrained")
    for label in top_profiles.profile.head(5):
        rows = annual[annual.profile == label].sort_values("collision_year")
        axis.plot(rows.collision_year, rows.share_of_all_fatal_or_serious_percentage,
                  marker="o", label=label)
    axis.set(xlabel="Year", ylabel="Share of all fatal or serious collisions in that year (%)",
             title="Pooled top-five profiles followed across individual years")
    axis.set_xticks(expected_years)
    axis.grid(alpha=0.2)
    axis.legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=8)
    fig.savefig(figures / "profile_years.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    monthly = pd.read_csv(path / "monthly.csv")
    monthly["period"] = pd.to_datetime(dict(year=monthly.collision_year, month=monthly.month, day=1))
    monthly = monthly.sort_values("period")
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True, layout="constrained")
    axes[0].plot(monthly.period, monthly.collision_count, color="#23708d")
    axes[0].set(ylabel="Reported collisions", title="Monthly reported collision volume and severity")
    axes[1].plot(monthly.period, monthly.fatal_or_serious_percentage, color="#ae4d36")
    axes[1].set(ylabel="Fatal or serious (%)", xlabel="Month")
    for axis in axes:
        axis.grid(alpha=0.2)
    fig.savefig(figures / "monthly.png", dpi=160)
    plt.close(fig)
    for name, label in [("weather", "weather_conditions_label"), ("lighting", "light_conditions_label"),
                        ("road_type", "road_type_label")]:
        table = pd.read_csv(path / f"{name}.csv").sort_values("fatal_or_serious_percentage")
        fig, axis = plt.subplots(figsize=(10, 6), layout="constrained")
        axis.barh(table[label], table.fatal_or_serious_percentage, color="#23708d")
        axis.set(xlabel="Fatal or serious collisions / reported collisions (%)",
                 title=f"Reported collision severity by {name.replace('_', ' ')}")
        axis.grid(axis="x", alpha=0.2)
        fig.savefig(figures / f"{name}.png", dpi=160)
        plt.close(fig)
    totals = report["source_totals"]
    severe_pct = totals["fatal_or_serious"] * 100 / totals["collisions"]
    summary = {"gold_run_id": report["run_id"], "silver_run_id": report["silver_run_id"],
               **totals, "fatal_or_serious_percentage": round(severe_pct, 3)}
    top5_count = int(profiles.head(5).fatal_or_serious_collision_count.sum())
    summary["profile_group_count"] = len(profiles)
    summary["top_five_severe_collision_count"] = top5_count
    summary["top_five_share_of_all_severe_percentage"] = (
        round(top5_count * 100 / totals["fatal_or_serious"], 3) if totals["fatal_or_serious"] else None)
    summary["top_profiles"] = json.loads(top_profiles.to_json(orient="records"))
    summary["annual_profile_checks"] = annual_checks
    summary["analytical_question"] = report["analytical_question"]
    write_json(output / "results.json", summary)
    top5_share = summary["top_five_share_of_all_severe_percentage"]
    top5_text = f"{top5_share:.3f}%" if top5_share is not None else "undefined (no severe collisions)"
    lines = ["# STATS19 — Analytical Results", "", f"Validated Gold run: `{report['run_id']}`.", "",
             f"The snapshot contains **{totals['collisions']:,} reported collisions**, "
             f"**{totals['casualties']:,} casualties**, and **{totals['fatal_or_serious']:,} "
             f"fatal or serious collisions ({severe_pct:.3f}%)**.", "",
             "**Question:** Which lighting, road-type and speed-limit profiles account for the largest "
             "numbers of fatal or serious reported collisions in Great Britain, 2021–2025?", "",
             "These are descriptive, unadjusted reported-severity figures. Percentages are "
             "conditional on a recorded collision, not traffic exposure or causal effects. "
             "Changing severity recording practices can affect comparisons over time.", "",
             "## Profiles accounting for the largest numbers", "",
             f"The five largest profiles contain **{top5_count:,} fatal or serious collisions**, "
             f"with an overall share of **{top5_text}**.", "",
             "| Profile | All reported collisions | Fatal or serious | Within-profile severe (%) | Share of all collisions (%) | Share of all severe (%) | Gap (percentage points) |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    answer = analytical_answer(summary)
    heading = lines.index("## Profiles accounting for the largest numbers")
    lines[heading:heading] = [answer, ""]
    for row in summary["top_profiles"]:
        share = row["share_of_all_fatal_or_serious_percentage"]
        share_text = f"{share:.3f}" if share is not None else "N/A"
        lines.append(f"| {row['profile']} | {row['collision_count']:,} | "
                     f"{row['fatal_or_serious_collision_count']:,} | {row['fatal_or_serious_percentage']:.3f} | "
                     f"{row['share_of_all_collisions_percentage']:.3f} | {share_text} | "
                     f"{row['severe_minus_all_collision_share_pp'] if share is not None else 'N/A'} |")
    largest = summary["top_profiles"][0]
    highest_share = max(summary["top_profiles"], key=lambda row: row["fatal_or_serious_percentage"])
    largest_share = largest["share_of_all_fatal_or_serious_percentage"]
    largest_share_text = f"{largest_share:.3f}% of all severe collisions" if largest_share is not None else "overall share undefined"
    largest_annual = annual_checks[0]
    contrast_annual = next(row for row in annual_checks if row["profile"] == highest_share["profile"])
    lines.extend(["", "![Profile counts and proportions](figures/collision_profiles.png)", "",
                  "## Comparison with the recorded-collision reference", "",
                  "The gap is a profile's share of all severe collisions minus its share of all "
                  "recorded collisions, in percentage points. A positive gap describes relative "
                  "over-representation among recorded severe cases; a negative gap describes "
                  "under-representation. This reference is not traffic exposure, and the gap is "
                  "not a causal effect, a risk ratio or a statistical significance test.", "",
                  "![Profile shares against the collision reference](figures/profile_reference.png)", "",
                  "## Analytical conclusions", "",
                  f"1. **Largest recorded burden:** {largest['profile']} accounts for "
                  f"{largest['fatal_or_serious_collision_count']:,} fatal or serious collisions "
                  f"({largest_share_text}). "
                  f"It also contains {largest['share_of_all_collisions_percentage']:.3f}% of all "
                  f"recorded collisions; its severe-minus-all gap is "
                  f"{largest['severe_minus_all_collision_share_pp']} percentage points. "
                  "A large absolute caseload does not alone indicate over-representation.", "",
                  f"2. **Concentration:** the five leading profiles together account for "
                  f"an overall share of {top5_text}. "
                  "This is a description of the recorded caseload, not an exposure-adjusted risk ranking.", "",
                  f"3. **Counts and proportions answer different questions:** among the ten profiles "
                  f"with the most severe collisions, the highest within-profile percentage is "
                  f"{highest_share['fatal_or_serious_percentage']:.3f}% for {highest_share['profile']}, "
                  f"based on {highest_share['collision_count']:,} reported collisions, including "
                  f"{highest_share['fatal_or_serious_collision_count']:,} severe cases. "
                  f"Its shares of all recorded collisions and all severe collisions are "
                  f"{highest_share['share_of_all_collisions_percentage']:.3f}% and "
                  f"{highest_share['share_of_all_fatal_or_serious_percentage']}%, respectively. "
                  "A high percentage alone does not establish the largest absolute burden.", "",
                  f"4. **Year-by-year check:** the pooled leading profile has annual severe-count "
                  f"ranks {largest_annual['rank_min']}–{largest_annual['rank_max']}, and a negative "
                  f"share gap in {largest_annual['negative_gap_years']} of {len(expected_years)} years. "
                  f"The contrasting profile has a positive share gap in "
                  f"{contrast_annual['positive_gap_years']} of {len(expected_years)} years. "
                  "These are descriptive sensitivity checks on the same period, not out-of-sample "
                  "validation. Reporting changes can still affect annual comparisons.", "",
                  "5. **Practical use:** an analyst can identify which profile groups warrant closer "
                  "local examination, then obtain location, traffic-exposure and infrastructure "
                  "information. These summaries do not determine an intervention or its effectiveness.", "",
                  "## Annual profile check", "",
                  "The five profiles are selected using the pooled period, then followed without "
                  "changing the selection each year. A profile absent in a year is not labelled stable. "
                  "Annual reference totals are reconciled separately to Silver.", "",
                  "![Selected profile shares by year](figures/profile_years.png)", "",
                  "## Supplementary temporal and environmental summaries", "",
                  "![Monthly collision volume and severity](figures/monthly.png)", ""])
    for name, label in [("weather", "weather_conditions_label"), ("lighting", "light_conditions_label"),
                        ("road_type", "road_type_label")]:
        lines.extend([f"## {name.replace('_', ' ').title()}", "",
                      "| Category | Reported collisions | Fatal or serious (%) |", "|---|---:|---:|"])
        with (path / f"{name}.csv").open() as handle:
            for row in csv.DictReader(handle):
                lines.append(f"| {row[label]} | {int(row['collision_count']):,} | {float(row['fatal_or_serious_percentage']):.3f} |")
        lines.extend(["", f"![Severity by {name}](figures/{name}.png)", ""])
    lines.extend(["## Interpretation limits", "",
                  "Small groups can yield unstable percentages. Unknown categories are retained, "
                  "and missing speed limits form a separate group. Vehicle-type groups may overlap: "
                  "a collision involving a car and a cycle appears in both groups. "
                  "The tables describe recorded personal injury collisions in Great Britain, "
                  "not all road accidents.", "",
                  "Source: [Department for Transport, STATS19](https://www.gov.uk/government/statistical-data-sets/road-safety-open-data). "
                  "See [severity guidance](https://www.gov.uk/government/publications/guide-to-severity-adjustments-for-reported-road-casualty-statistics) "
                  "before interpreting time trends.", ""])
    (output / "results.md").write_text("\n".join(lines))
    print(f"Presentation written to {output}")
    return summary


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--output", type=Path, default=root / "reports")
    parser.add_argument("--gold", type=Path, help="Override the Gold output directory")
    args = parser.parse_args()
    settings = json.loads((root / "config" / f"{args.env}.json").read_text())
    run(args.gold or root / settings["gold"], args.output)
