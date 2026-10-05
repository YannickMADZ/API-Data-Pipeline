# Pipeline NYC Yellow Taxi — API Socrata → données propres et validées

## 1. Objectif

Ce projet construit un pipeline automatique qui, pour un **mois donné** (année + mois en paramètres) :

1. **extrait** l'intégralité des trajets Yellow Taxi depuis l'API Socrata de NYC Open Data (dataset `4b4i-vvec`, 2023) ;
2. **transforme** les données : typage, valeurs manquantes, anomalies, puis sauvegarde en CSV ;
3. **valide** le résultat par des contrôles chiffrés et produit un rapport qualité.

Les trois étapes s'enchaînent avec **une seule commande**. Si une étape échoue, le pipeline s'arrête et les étapes suivantes ne s'exécutent pas.

## 2. Prérequis

- Un compte sur [data.cityofnewyork.us](https://data.cityofnewyork.us) et un **App Token** Socrata (profil développeur → clés API).
- Au choix :
  - **Docker** et Docker Compose (recommandé) ;
  - ou **Python 3.10+** en local.

## 3. Installation

```bash
git clone <url-du-depot>
cd nyc-taxi-pipeline

# Le token : copier le modèle puis renseigner SOCRATA_APP_TOKEN
cp .env.example .env
```

Le fichier `.env` est listé dans `.gitignore` et `.dockerignore` : il n'est **jamais versionné** ni copié dans l'image Docker.

**Avec Docker :**
```bash
docker compose build
```

**Sans Docker :**
```bash
python -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

## 4. Lancement

### Pipeline complet

```bash
# Docker
docker compose run --rm pipeline --year 2023 --month 1

# Local
python -m pipeline --year 2023 --month 1
```

### Options

| Option | Rôle | Exemple |
|---|---|---|
| `--year`, `--month` | Mois à traiter (obligatoires) | `--year 2023 --month 7` |
| `--steps` | Étapes à exécuter, dans l'ordre officiel | `--steps extract` |
| `--force` | Ignore les fichiers existants du mois et repart de zéro | `--force` |
| `--config` | Autre fichier de configuration | `--config config.yaml` |
| `--log-level` | Niveau de détail des logs | `--log-level DEBUG` |

Pour traiter un autre mois, il suffit de changer `--year` / `--month` : **aucune modification de code**.

### Codes de sortie

| Code | Signification |
|---|---|
| `0` | Succès |
| `1` | Une étape a échoué (détails dans `logs/`) |
| `2` | Erreur de configuration (paramètre invalide, token absent...) |
| `130` | Interruption manuelle (Ctrl+C) : relancer la même commande reprend l'extraction |

### Notebook d'exploration

```bash
# Extraire d'abord le mois à explorer
docker compose run --rm pipeline --year 2023 --month 1 --steps extract

# Puis lancer JupyterLab
docker compose up notebook
# -> ouvrir http://localhost:8888 puis notebooks/01_exploration.ipynb
```

Le notebook peut aussi interroger directement l'API pour un échantillon (`SOURCE = "api"`, taille fixée par `SAMPLE_LIMIT`).

## 5. Arborescence

```
nyc-taxi-pipeline/
├── .env.example            # modèle du fichier secret (sans valeur)
├── .gitignore / .dockerignore
├── Dockerfile              # image unique : pipeline + notebook
├── docker-compose.yml      # services "pipeline" et "notebook"
├── config.yaml             # TOUS les paramètres : API, $limit, réseau, chemins, schéma, seuils
├── requirements.txt
│
├── pipeline/               # code de production (package Python)
│   ├── __main__.py         # orchestrateur : point d'entrée, enchaîne les étapes
│   ├── config.py           # lecture/validation de config.yaml, du token et du mois
│   ├── logger.py           # journalisation console + fichier
│   ├── io_utils.py         # écriture atomique, lecture JSON / JSON.gz
│   ├── extract.py          # étape 1 : count(*), pagination, retry, reprise
│   ├── transform.py        # étape 2 : typage, suppression, marquage, CSV
│   └── validate.py         # étape 3 : contrôles chiffrés, rapport qualité
│
├── notebooks/
│   └── 01_exploration.ipynb   # analyse justifiant les seuils (sorties nettoyées)
│
├── data/                   # NON versionné — régénéré par le pipeline
│   ├── raw/AAAA-MM/        # page_00000.json.gz ... + _manifest.json
│   └── processed/AAAA-MM/  # trips_clean.csv
│
├── reports/AAAA-MM/        # VERSIONNÉ — preuves légères
│   ├── extract_summary.json    # count API, lignes extraites, doublons
│   ├── transform_summary.json  # détail technique de la transformation
│   └── quality_report.json     # LE rapport qualité (voir §8)
│
└── logs/                   # NON versionné — pipeline_AAAA-MM.log
```

**Règle de rangement** : le **code** est dans `pipeline/`, les **paramètres** dans `config.yaml`, les **données régénérables** dans `data/`, les **preuves légères** dans `reports/`. Tout est partitionné par mois (`AAAA-MM`), donc plusieurs mois coexistent sans s'écraser.

**Ajouter une étape** (par exemple un chargement en base) : créer `pipeline/load.py` avec une fonction `run(cfg, period, force)`, puis ajouter une ligne dans le dictionnaire `STEPS` de `__main__.py`. Rien d'autre ne bouge.

## 6. Choix techniques et justifications

### 6.1 Pagination : `$limit`, `$offset`, `$order=:id`

- **Pourquoi paginer ?** Sans `$limit`, l'API renvoie seulement 1000 lignes, sans erreur. Un mois compte plusieurs millions de trajets : il faut boucler sur `$offset`.
- **Pourquoi `$order=:id` ?** Sans tri explicite, l'ordre des lignes n'est pas garanti d'une requête à l'autre. Paginer par `$offset` produirait alors **doublons et trous**. On l'a constaté dès les premiers appels : les lignes arrivent dans un ordre arbitraire (heures de pickup mélangées). `:id` est l'identifiant système unique de chaque ligne : il donne un ordre total et stable.
- **Pourquoi `$select=*, :id` ?** `:id` n'est pas renvoyé par défaut. On le demande explicitement pour **vérifier l'absence de doublons** dans les fichiers bruts. L'ordre compte : la première version (`:id,*`) a été refusée par l'API (HTTP 400, *« Star selections must come first »*), et le pipeline s'est arrêté immédiatement sans réessayer, ce qui est le comportement voulu pour une erreur définitive.
- **Pourquoi `$limit = 50000` ?** C'est un compromis entre deux risques. Avec une petite page (1000), il faudrait des milliers de requêtes pour un mois : c'est lent et ça consomme du quota. Avec une très grande page, les réponses sont lourdes, le risque de timeout augmente, et une page perdue coûte cher à retélécharger. 50 000 donne une soixantaine de requêtes par mois.
  Pour janvier 2023 (3 066 726 lignes) : 62 pages.
- **Complétude** : avant l'extraction, on demande à l'API un `count(*)` avec **exactement le même filtre `$where`**. On calcule alors le nombre de pages attendu et le nombre de lignes attendu par page. Chaque page reçue est contrôlée **avant** d'être écrite, et le total final doit égaler le `count(*)`. Sinon, le pipeline s'arrête.
- **Filtre du mois** : `date >= début du mois AND date < début du mois suivant`. La borne haute est exclue pour qu'un trajet à minuit pile ne soit jamais compté dans deux mois, et on n'a pas à gérer les mois de 28, 30 ou 31 jours.

### 6.2 Gestion des erreurs réseau

- **Timeout séparé connexion / lecture (10 s / 120 s).** Une connexion qui n'aboutit pas en 10 s signale un problème réseau : inutile d'attendre davantage. La lecture, elle, peut légitimement être longue, et c'est mesuré : lors de l'extraction de janvier 2023, **le simple `count(*)` a mis 41 secondes** à répondre (l'API filtre des millions de lignes). Un timeout de lecture de 30 s aurait fait échouer ce comptage à chaque exécution ; 120 s laisse une marge d'environ 3× sur le temps observé.
- **Tentatives avec attente exponentielle** (2 s, 4 s, 8 s, … plafonnée à 60 s, 5 tentatives) sur les erreurs **transitoires** : coupure réseau, timeout, HTTP 429 (limite de débit), 5xx (serveur surchargé), JSON tronqué. L'attente croissante laisse au serveur le temps de récupérer au lieu de le resolliciter immédiatement. Un léger aléa (*jitter*) évite de relancer toujours au même instant. Si l'API envoie un en-tête `Retry-After`, il est respecté.
- **Échec immédiat sur les erreurs définitives** (400 requête invalide, 403 token refusé…) : réessayer ne changerait rien et masquerait le vrai problème.
- **Token dans l'en-tête `X-App-Token`**, pas dans l'URL : les URL apparaissent dans les logs et messages d'erreur, pas les en-têtes.

### 6.3 Écriture atomique et reprise après interruption

- Chaque fichier est d'abord écrit dans un temporaire du même dossier, synchronisé sur disque (`fsync`), puis renommé avec `os.replace()`. Ce renommage est atomique : **un fichier final existe en entier ou pas du tout**, même en cas de Ctrl+C ou de crash.
- Conséquence directe : **un fichier de page présent est forcément complet**. Si le pipeline est relancé après une interruption, les pages existantes sont relues et recontrôlées au lieu d'être retéléchargées. Pour un mois de ~60 pages, une coupure à la page 55 ne coûte que les 5 pages restantes.
- Un `_manifest.json` mémorise les paramètres de l'extraction (filtre, `$limit`, count). Si on relance avec une autre taille de page, les offsets ne correspondent plus : le pipeline refuse de mélanger des pages incompatibles et demande `--force`.

### 6.4 Format de sauvegarde

- **Pages brutes en JSON compressé (`.json.gz`)** : on conserve la réponse de l'API **telle quelle**, sans perte, ce qui permet de rejouer la transformation sans réinterroger l'API. Le JSON compressé est environ 10 fois plus léger que le JSON brut.
- **Données nettoyées en CSV** (`trips_clean.csv`) : format demandé par la consigne, lisible par tous les outils (Excel, pandas, SQL). Contrepartie : le CSV ne stocke pas les types. Les dates sont écrites en ISO (`2023-01-01 00:32:10`), les entiers manquants restent des cellules vides (pas de `1.0` ni de `0` inventé), et les indicateurs valent `True` / `False`. Les colonnes suivent l'ordre du dictionnaire TLC (`config.yaml` → `output.column_order`), puis viennent les colonnes dérivées (`duration_min`, `speed_mph`, `total_gap`) et les indicateurs `flag_*`.
- **Ressources** : la transformation charge le mois en mémoire (environ 900 Mo une fois typé, 2 à 3 Go au pic). Les pages sont typées une par une avant concaténation, ce qui évite de garder 3 millions de lignes sous forme de chaînes.
- **Données non versionnées** : plusieurs centaines de Mo par mois. Elles se régénèrent avec `python -m pipeline --year AAAA --month MM`. Seuls les rapports (quelques Ko) sont versionnés.

### 6.5 Arrêt sur échec

L'orchestrateur exécute les étapes dans l'ordre et s'arrête **à la première exception**, avec un code de sortie non nul. Les étapes suivantes ne tournent donc jamais sur des données incomplètes. Chaque étape vérifie aussi que la précédente a réussi : `transform` refuse de démarrer sans un `extract_summary.json` au statut `OK`, et recompte les lignes brutes avant de commencer.

### 6.6 Journalisation

Chaque étape journalise son début, sa progression (page par page pour l'extraction), ses contrôles et sa durée, dans la console et dans `logs/pipeline_AAAA-MM.log`. En cas d'échec, la trace complète est écrite dans le fichier, ce qui permet de comprendre une erreur sans relire le code.

## 7. Seuils de nettoyage et anomalies

Toutes les observations ci-dessous viennent de l'exploration du **mois complet de janvier 2023 (3 066 726 lignes)** dans `notebooks/01_exploration.ipynb`. Les seuils sont dans `config.yaml` (section `cleaning`). À chaque exécution, le rapport qualité **recalcule les percentiles du mois traité**, ce qui permet de vérifier qu'un seuil reste pertinent sur un autre mois.

### 7.1 Principe : supprimer l'impossible, marquer le suspect

Une ligne n'est **supprimée** que si le trajet est **physiquement impossible**. Une ligne **suspecte mais correspondant à une transaction réelle** est **conservée** avec une colonne `flag_*` : la supprimer fausserait les revenus et les volumes, alors qu'un indicateur laisse chaque analyse décider.

### 7.2 Lignes supprimées

Les règles s'appliquent dans l'ordre, chacune sur les lignes restantes : une ligne n'est comptée que pour une raison, et la somme des compteurs donne le total retiré. Les nombres exacts par règle sont dans `reports/2023-01/quality_report.json`.

| Règle | Observation chiffrée (janvier 2023) | Seuil | Pourquoi ce seuil |
|---|---|---|---|
| Pickup hors du mois | 0 ligne | `[début ; fin[` du mois | Garde-fou : le `$where` de l'extraction les exclut déjà. |
| Durée ≤ 0 | 3 durées négatives, 1 118 nulles | `> 0 min` | Dépôt avant ou au même instant que la prise en charge : impossible. |
| Durée trop longue | p99 = 57 min, **p99.9 = 175 min**, max = **10 029 min (7 jours)** | `≤ 180 min` | Seuil collé au p99.9 (arrondi à 3 h). Au-delà, la queue s'étire sur des jours : compteur resté allumé, pas un trajet. |
| Distance trop longue | p99 = 20,1 mi, **p99.9 = 28,8 mi**, max = **258 928 mi** | `≤ 100 mi` | Le seuil n'est **pas** le p99.9 : couper à 28,8 mi supprimerait des courses légitimes (aéroports, Long Island, New Jersey). 100 mi (environ 3,5 × p99.9) ne retire que des distances sans rapport avec un taxi. |
| Vitesse moyenne impossible | p99 = 38,2 mph, **p99.9 = 49,8 mph**, max = **2 589 281 mph** | `≤ 80 mph` | 99,9 % des trajets roulent en moyenne sous 50 mph, puis la distribution explose. Une **moyenne** sur tout le trajet au-delà de 80 mph (au-dessus des limitations des voies rapides de New York, 65 mph au plus) signale une distance ou des horodatages faux. Ce filtre attrape les incohérences que les seuils de distance et de durée seuls laissent passer. |

### 7.3 Anomalies conservées volontairement

| Indicateur | Observation (janvier 2023) | Pourquoi conserver |
|---|---|---|
| `flag_extra_includes_surcharges` — **l'anomalie piège** | `total_amount` ≠ somme des composantes pour **27 % des lignes**. Pour le vendor 1, l'écart vaut **exactement −(congestion + aéroport)** dans **762 661 cas sur 827 367 (92 %)** : −2,50 (706 579), −1,25 (26 685), −3,75 (29 397). | Le total facturé est **juste** : c'est le champ `extra` du vendor 1 qui inclut déjà ces surcharges, ensuite répétées dans leurs propres colonnes. Les supprimer retirerait un quart du mois sur une simple convention de saisie. Conséquence documentée : ne jamais additionner `extra` + surcharges pour le vendor 1. |
| `flag_total_mismatch_other` | Écarts restants : surtout **+2,50** (43 650 lignes vendor 2, 22 769 vendor 1). | Écart non expliqué par la règle ci-dessus. Le rapport indique combien de ces lignes ont aussi des métadonnées manquantes (surcharge présente dans le total mais colonne vide). Le total reste la référence. |
| `flag_missing_metadata` | **71 743 lignes (2,3 %)**. Les 5 colonnes (`passenger_count`, `ratecodeid`, `store_and_fwd_flag`, `congestion_surcharge`, `airport_fee`) sont manquantes **toujours ensemble**, et **100 %** de ces lignes ont `payment_type = 0`. Montant médian = 24,05 $. | Valeurs **non aléatoires** : ce sont des trajets réels dont seules les métadonnées du véhicule manquent. **Pas d'imputation** : une médiane inventerait des passagers, et un 0 affirmerait qu'aucune surcharge n'a été facturée. |
| `flag_negative_amount` | **25 204 lignes (0,8 %)**, dont 14 751 litiges (`payment_type = 4`, soit 44 % des litiges) et 4 532 sans frais (`= 3`). Seulement 21 en carte. | Contre-passations comptables (annulations, remboursements), conservées pour l'audit. À exclure d'une analyse de revenus. |
| `flag_zero_distance` | **45 856 lignes (1,5 %)** | Course payée mais distance nulle : défaillance GPS probable, transaction réelle. À exclure des analyses de distance ou de vitesse. |
| `flag_zero_passenger` | **51 164 lignes**, dont **50 988 chez le vendor 1 (99,7 %)** | Saisie manuelle non faite par le chauffeur, pas un trajet sans passager. |
| `flag_unknown_zone` | 41 761 prises en charge et 33 544 déposes en zone 264/265 | Zones « Unknown » / « N/A » de la table TLC. Montants valides : à exclure seulement des analyses géographiques. |

### 7.4 Typage

L'API renvoie **toutes** les valeurs sous forme de chaînes, y compris les entiers sous la forme `"1.0"`. Chaque colonne est convertie explicitement (`config.yaml` → `schema`), et les entiers utilisent le type `Int64` de pandas, le seul qui accepte les valeurs manquantes sans repasser en flottant. Toute conversion ratée est comptée dans le rapport : sur janvier 2023, il y en a **0**, et aucune valeur entière n'a de partie décimale.

## 8. Rapport qualité

Le fichier `reports/AAAA-MM/quality_report.json` contient :

- `status` : `VALIDE` ou `ECHEC` ;
- `summary` : count de l'API, lignes extraites, retirées (nombre et %), finales ;
- `thresholds` : les seuils utilisés ;
- `removed_rows_by_rule` : pour chaque règle, le seuil, le nombre de lignes retirées, **les percentiles observés sur le mois** et la justification ;
- `kept_despite_anomaly` : chaque anomalie conservée, son volume, sa répartition (par vendor ou type de paiement) et la justification ;
- `type_conversion_failures`, `missing_values_before_cleaning` ;
- `checks` : les 7 contrôles de validation avec leur résultat.

Les 7 contrôles relisent le **CSV écrit sur disque**, c'est-à-dire ce qui est réellement livré :

| Contrôle | Vérifie que |
|---|---|
| `extraction_complete` | lignes extraites = `count(*)` de l'API, 0 doublon de `:id` |
| `conservation_des_lignes` | brutes = finales + somme des retirées par règle |
| `taille_du_csv` | le CSV contient exactement le nombre de lignes annoncé |
| `unicite_row_id` | aucun doublon dans le fichier final |
| `seuils_respectes` | aucune ligne finale ne viole un seuil de suppression |
| `pickup_dans_le_mois` | toutes les prises en charge sont dans le mois traité |
| `colonnes_presentes` | les colonnes attendues sont présentes, dans l'ordre |

Si un contrôle échoue, le rapport est quand même écrit (avec `status: ECHEC`) pour permettre le diagnostic, puis le pipeline s'arrête avec le code de sortie 1.

## 9. Sécurité des secrets

- Le token est lu depuis `.env`, jamais écrit dans le code.
- `.env` est exclu de Git (`.gitignore`) et de l'image Docker (`.dockerignore`). Il est injecté au lancement via `env_file`.
- Vérification avant chaque push :
  ```bash
  git log --all --full-history -- .env       # doit être vide
  git grep -i "<début de ton token>" $(git rev-list --all)   # doit être vide
  ```
- Avant de committer le notebook, il faut nettoyer ses sorties :
  `jupyter nbconvert --clear-output --inplace notebooks/01_exploration.ipynb`
