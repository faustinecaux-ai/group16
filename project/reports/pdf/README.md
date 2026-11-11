# Submission report

`report.pdf` is the five-page English report for Group 16. `report.tex` is its editable LaTeX source. It summarises the merged implementation, the recorded development run, the retained production manifest, and the current workflow configuration.

From this directory, with TeX Live's base and recommended LaTeX packages installed:

```bash
pdflatex -interaction=nonstopmode -halt-on-error report.tex
pdflatex -interaction=nonstopmode -halt-on-error report.tex
```

Figures are referenced from `../figures/`. Commit the PDF and source together after any update. Auxiliary compilation files are ignored. The report is a dated account of the evidence, not an automatically refreshed pipeline output.
