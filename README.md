# Déploiement de la base de données OpenAlex
Journal du processus de déploiement de l'intégralité de la base de données OpenAlex sur l'environnement Nibi.

Infrastructure: Nibi (Cluster de l'alliance de recherche numérique du Canada)

Lien: [https://docs.alliancecan.ca/wiki/Nibi/fr]

## 1. Allocation de stockage
Demande de 5 To d'espace de projet permanent via le Service d'accès rapide de l'Alliance.

Lien: [https://docs.alliancecan.ca/wiki/Rapid_Access_Service/fr]

## 2. Connection à Nibi et installation de paquets python
Dans le terminal, se connecter dans l'environnement Nibi à l'aide de ssh:
```bash
ssh nom_utilisateur_ccdb@computecanada.ca
```
Changer de dossier vers le projet de votre équipe:
```bash
cd $project
cd projects
```
Puis changer de nouveau dans le dossier du nom du groupe:
```bash
cd def-nom_du_groupe
```
Ensuite créer un nouveau dossier pour le téléchargement:
```bash
mkdir openalex_snapshot
cd openalex_snapshot
```
Ouvrir une session virtuelle, pour garantir le téléchargement sans interruptions:
```bash
tmux
```
Installer l'outil de aws, le module "Arrow" et le paquet DuckDB dans un environnement virtuel:
```bash
module load gcc arrow
```
```bash
module load python/3.11.5
```
```bash
virtualenv data_env
source data_env/bin/activate
```
```bash
pip install awscli pyarrow duckdb
```
## 3. Téléchargement des données comprimées
Vérifier la taille du snapshot (seulement la section données, sans "legacy-data") :
``` {bash}
aws s3 ls --summarize --human-readable --no-sign-request --recursive "s3://openalex/data"
```
Télécharger:
```{bash}
aws s3 sync "s3://openalex/data" "data" --no-sign-request
```
Lien: [https://developers.openalex.org/download/download-to-machine]

## 3. Décompression et transformation vers .parquet
Les fichiers doivent ensuite être transformés à partir du format JSON au format parquet (stockage de données en colonnes, qui permet d'utiliser DuckDB pour l'analyse SQL).

### 3.1 Préparer le script
Le script Python [flatten-openalex-parquet.py](flatten-openalex-parquet.py)  a été créé à l'aide de Claude AI.

Il utilise la même structure relationelle que le script développé par OpenAlex [https://github.com/ourresearch/openalex-documentation-scripts] (qui transforme en .csv) . 

Copier le script dans le projet: 
```bash
vim flatten-openalex-parquet.py
```
Copier-coller le script au complet dans la fenêtre vim du terminal, puis enregistrer avec :wq, enter.

### 3.2 Préparer la tâche
Il faut ensuite soumettre la tâche à la grappe Nibi, ce qu'on peut faire avec la commande sbatch et le  script Slurm [tache_conversion.sh](tache_conversion.sh) .

Pour plus d'information sur sbatch: [https://docs.alliancecan.ca/wiki/Running_jobs/fr]

Commencer par copier le script Slurm dans le projet de la même façon qu'avec le script Python.

Ensuite, soumettre la tâche:
```bash
sbatch tache_conversion.sh
```
## 4. Configuration de Open On Demand

## 5. Mise à jour du snapshot




