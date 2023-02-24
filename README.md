# Données spatiales utilisées à HEC

Le [wiki](https://github.com/ElodieFZ/Produits_Spatiaux/wiki) décrit quelles données sont utilisées à HEC.

# Traitement de données spatiales

## Tutoriaux

Tous les tutoriaux sont disponibles sous forme de notebooks dans `python/notebooks`. Une version HTML est également disponible dans `python/docs`.

 * Introduction à Python spatial ([HTML](https://elodiefz.github.io/Produits_Spatiaux/python/docs/introduction.html))
 * Utiliser Google Earth Engine (GEE) ([HTML](https://elodiefz.github.io/Produits_Spatiaux/python/docs/introduction_gee.html))

## Scripts


## Installation

 * Python
 
Pour pouvoir utiliser les notebooks jupyter et les scripts python, il faut installer python et les librairies nécessaires. Il est conseillé d'utiliser Anaconda ([Installer Anaconda](https://docs.anaconda.com/anaconda/install/index.html)) ou miniconda ([Installer miniconda](https://docs.conda.io/en/latest/miniconda.html)), une version légère d'Anaconda.
Une fois Anaconda installé,

pour créer un nouvel environnement contenant tous les modules nécessaires :

```text
conda create --name <mon_nouvel_environment> -c conda-forge --file python/requirements.txt
```

ou pour installer les modules nécessaires dans un environnement déjà existant :

```
conda activate <mon_environnement_existant>
conda install -c conda-forge --file python/requirements.txt
```


