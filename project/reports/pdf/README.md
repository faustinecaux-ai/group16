# Project report

The six-page English report is authored by **Faustine Caux and Jules Gombert**, Group 16, **University of Stavanger**, for DAT535.

- [report.pdf](report.pdf): assessment report, centred on Silver preparation, relational integrity and validation, with a focused Gold application.
- [report.tex](report.tex): editable LaTeX source.

The report uses the recorded development run, the retained production manifest and current workflow configuration. Versioned JSON evidence supports the figures and numerical results. It is a dated engineering account rather than an automatically refreshed pipeline report.

## Build

The figure script uses matplotlib and the versioned Silver and results JSON files and the annual-profile CSV export; it requires no raw CSVs or Spark. From the `project` directory, in the presentation environment:

```bash
python scripts/render_report_figures.py
cd reports/pdf
pdflatex -interaction=nonstopmode -halt-on-error report.tex
pdflatex -interaction=nonstopmode -halt-on-error report.tex
```

The LaTeX environment requires TeX Live's base and recommended packages plus Latin Modern fonts (`texlive-latex-base`, `texlive-latex-recommended`, `lmodern` on Ubuntu). Figures are referenced from `../figures/`. PDF, source and figures are versioned together; auxiliary compilation files are ignored.
