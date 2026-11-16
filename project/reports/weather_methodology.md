# Extension Silver : reconstruction indépendante de la météo

Cette extension répond au retour de soutenance : STATS19 contient déjà des catégories préparées. Les travaux initiaux sont conservés ; une source indépendante et une préparation spatio-temporelle sont ajoutées. Le PDF et le notebook initiaux décrivent la version précédente.

## Source et périmètre

Source : [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), modèle **ERA5 explicitement sélectionné**, réanalyse ECMWF/Copernicus. Attribution : Open-Meteo et Copernicus/ECMWF ERA5 ; données sous CC BY 4.0. Les JSON originaux, URL, dates de récupération, coordonnées retournées et SHA-256 sont archivés dans `data/bronze/weather`. Il s'agit d'estimations de réanalyse, pas d'observations sur chaque lieu d'accident.

Périmètre exécuté : boîte londonienne (latitude 51,25–51,75, longitude −0,50–0,25), année 2023 entière, 12 cellules de requête de 0,25°. Cette boîte ne correspond pas aux frontières administratives de Londres. Tous les autres accidents restent présents avec le statut `outside_scope`. La couverture locale doit être distinguée de la couverture nationale. Les bornes sont configurables dans `config/weather.json` ; une extension nécessite une nouvelle acquisition et une nouvelle exécution.

## Travail de préparation

1. Les cellules occupées par les accidents déterminent les requêtes. Chaque réponse couvre la période avec un jour supplémentaire à chaque extrémité. L'acquisition reprend les fichiers existants et vérifie leurs empreintes.
2. Silver vérifie SHA-256, UTC, unités, longueurs des tableaux JSON, valeurs non finies, vent/précipitations négatifs et unicité `(cellule, heure UTC)`. Les tableaux deviennent une table `weather_hourly` en Parquet avec provenance par ligne.
3. Conversion des heures STATS19 depuis `Europe/London`. Une conversion aller-retour avec les deux offsets possibles détecte les heures ambiguës et inexistantes aux changements d'heure. Elles ne reçoivent pas arbitrairement une météo.
4. Jointure gauche à l'heure UTC la plus proche, à 30 minutes maximum ; en cas d'égalité, l'heure suivante est retenue. Il s'agit d'un enrichissement rétrospectif pouvant utiliser une valeur postérieure à l'accident. Les précipitations correspondent au cumul de l'heure précédant le timestamp météo, pas à une mesure à la minute exacte de l'accident.
5. Distance de Haversine jusqu'aux coordonnées **réellement retournées** par l'API. Au-delà de 30 km, la catégorie reconstruite reste nulle. Les statuts distinguent hors périmètre, problèmes horaires, absence de météo, données inutilisables et distance excessive.
6. La météo policière n'intervient pas dans la reconstruction. Elle sert uniquement à la comparaison. Le contrôle de cardinalité interdit de multiplier ou supprimer les accidents. Les erreurs d'intégrité bloquent la publication ; les correspondances manquantes, trop lointaines ou inutilisables nécessitent une revue. Les exclusions liées au changement d'heure sont documentées.

## Règle `era5-weather-v1`

| Codes WMO | Phénomène | Code STATS19 approché |
|---|---|---|
| 0, 1, 2, 3 | absence de pluie/neige, couverture nuageuse variable | 1 ou 4 |
| 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82 | bruine/pluie, dont verglaçante | 2 ou 5 |
| 71, 73, 75, 77, 85, 86 | neige | 3 ou 6 |
| 45, 48 | brouillard | 7 |
| 95, 96, 99 | orage/autre | 8 |
| absent/non reconnu | inconnu | NULL |

Pour les trois premières familles, le second code s'applique si le vent moyen à 10 m atteint **40 km/h**. Ce seuil est un choix du projet, pas une équivalence officielle avec « high winds ». La catégorie dérivée et son libellé sont ajoutés dans `weather_conditions_reconstructed` et `weather_conditions_reconstructed_label` ; le code policier original reste inchangé. La résolution ERA5 limite la représentation des phénomènes locaux et l'API signale des limites dans l'estimation des orages.

Le taux d'accord utilise les seuls accidents appariés avec code policier 1–7. Les codes policiers 8 (autre), 9 (inconnu) et −1 restent présents mais sont exclus de ce dénominateur. Un désaccord ne prouve pas une erreur policière. Le seuil, la distance maximale et la version des règles sont tracés dans le rapport.

## Reproduction et sorties

```bash
python3 scripts/ingest_weather.py
bash scripts/run_tests.sh
bash scripts/run_project.sh --env dev
```

Seule l'acquisition utilise le réseau. Pour un autre périmètre : `--config` et `--output` dans l'acquisition puis `--weather-bronze` dans le pipeline. Le manifeste acquis contient les règles faisant autorité pour cette exécution. Les fichiers sources sont à conserver pour reproduire exactement les résultats.

Le rapport Silver fournit couverture, exclusions, dénominateur comparable, accord et matrice de confusion. Gold ajoute `weather_reconstructed.csv` et `weather_comparison.csv`. Les groupes non appariés restent présents et les totaux se réconcilient avec **tous** les accidents Silver. Les analyses météo utilisent le sous-ensemble comparable, sans interprétation causale ni estimation du risque routier.

## Pistes supplémentaires, non comptées comme réalisées

- Sensibilité aux seuils de vent et de distance sur la même archive.
- Extension géographique avec couverture par année et région.
- Reconstruction jour/nuit à partir de la position du soleil ; l'éclairage public reste une information distincte.
- Conversion British National Grid → WGS84 pour contrôler indépendamment les coordonnées fournies.
