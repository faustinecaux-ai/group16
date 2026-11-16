# Résultats de l'extension météo Silver

Exécution validée : `20261116T133604389268Z`. Bronze → Silver → Gold : passed.

| Indicateur | Résultat |
|---|---:|
| Accidents conservés (Grande-Bretagne, 2021–2025) | 513,801 |
| Dans la boîte londonienne, année 2023 | 25,695 |
| Météo reconstruite et appariée | 25,689 |
| Couverture du périmètre choisi | 99.977 % |
| Couverture de l'ensemble national | 5.000 % |
| Comparables avec un code policier 1–7 | 22,577 |
| Accords avec le code policier | 18,407 |
| Taux d'accord parmi les comparables | 81.53 % |
| Distance maximale observée au point météo | 16.251 km |
| Écart temporel maximal | 30.0 min |

## Statuts de rapprochement

- `ambiguous_local_time` : 6
- `matched` : 25,689
- `outside_scope` : 488,106

## Lecture des résultats

L'accord mesure la concordance entre deux descriptions différentes : un relevé policier local et une réanalyse horaire sur grille. Un désaccord ne désigne pas une erreur certaine de l'une des sources. Le seuil de vent de 40 km/h est une convention du projet. La couverture est limitée au périmètre londonien de 2023 et ne doit pas être présentée comme un enrichissement météo national complet.

Le travail Silver ajouté comprend acquisition traçable, normalisation JSON, contrôle des unités et clés, gestion des changements d'heure, rapprochement spatial et temporel, reconstruction indépendante, statuts d'exclusion et analyse de désaccord. Les totaux des deux nouvelles tables Gold se réconcilient avec tous les accidents, y compris ceux hors périmètre.

[Méthodologie](weather_methodology.md) · [Métriques et matrice de confusion](weather_quality.json) · [Comparaison Gold](weather_comparison.csv) · [Notebook](../notebooks/weather_extension.ipynb) · [Complément PDF](pdf/weather_extension.pdf)
