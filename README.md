# Déploiement de la base de données OpenAlex
Journal du processus de déploiement de l'intégralité de la base de données OpenAlex sur l'environnement Nibi.

Infrastructure: Nibi (Cluster de l'alliance de recherche numérique du Canada)

Lien: [https://docs.alliancecan.ca/wiki/Nibi/fr]

## 1. Allocation de stockage
Demande de 5 To d'espace de projet permanent via le Service d'accès rapide de l'Alliance.

Lien: [https://docs.alliancecan.ca/wiki/Rapid_Access_Service/fr]

## 2. Téléchargement des données comprimées
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
Installer l'outil de aws dans un environnement virtuel:
```bash
module load python/3.11.5
```
```bash
virtualenv aws_env
source aws_env/bin/activate
```
```bash
pip install awscli
```
Vérifier la taille du snapshot (seulement la section données, sans "legacy-data") :
``` {bash}
aws s3 ls --summarize --human-readable --no-sign-request --recursive "s3://openalex/data"
```
Télécharger:
```{bash}
aws s3 sync "s3://openalex/data" "data" --no-sign-request
```
Lien: [https://developers.openalex.org/download/download-to-machine]

## 3. Transformation vers Parquet
Les fichiers doivent ensuite être transformés à partir du format JSON au format parquet (stockage de données en colonnes, qui permet d'utiliser DuckDB pour l'analyse SQL).
L'approche standard avec .csv utilise le script Python (flatten-openalex-jsonl.py) , duquel on peut conserver la structure relationnelle.

Lien: [https://github.com/ourresearch/openalex-documentation-scripts]

Exemple de transformation DuckDB, pour transformer en parquet :

``` SQL
COPY (
    SELECT * FROM read_json_auto('openalex-snapshot/data/works/*/*.gz')
) TO 'works.parquet' (FORMAT PARQUET);
```



