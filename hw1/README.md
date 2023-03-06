# Homework 1
Hai Hung Nguyen

## Struktura
Řešení obsahuje dvě složky, Care Providers a Population 2021. Poté složka constraints, omezení
kde v složce jsou jednotlivé constraints podle [W3C](https://www.w3.org/TR/vocab-data-cube/#h3_wf-rules) spolu se 
skriptem `check_datacube.py` který jednotlivé constraints kontroluje. Také obsahuje `LICENSE` a `requirements.txt`.

Úkolem bylo vytvořit z daného datasetu data cube.
## Requirements
Packages:
```commandline
et-xmlfile==1.1.0
html5lib==1.1
isodate==0.6.1
numpy==1.24.2
openpyxl==3.1.1
owlrl==6.0.2
packaging==23.0
pandas==1.5.3
prettytable==2.5.0
pyparsing==3.0.9
pyshacl==0.20.0
python-dateutil==2.8.2
pytz==2022.7.1
rdflib==6.2.0
six==1.16.0
wcwidth==0.2.6
webencodings==0.5.1
```
Data Sets:
- [Care Providers Dataset](https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv)
- [Population in Czech Republic 2021](https://www.czso.cz/documents/10180/165603907/13007221n01.xlsx/65344c95-18ed-4020-a866-868ba56e52e5?version=1.2)

## Instalace
Abychom si balíčky neninstalovali globálně tak si vytvoříme venv
```commandline
virtualenv .venv
```

```commandline
./.venv/Scripts/activate
```

```commandline
pip install -r requirements.txt
```

## Care Providers
Adresář obsahuje dataset a skript `care_providers.py`. Výstup se ukládá do `out/care_providers.ttl`

## Population 2021
Adresář obsahuje dataset a skript `population.py`. Výstup se ukládá do `out/population.ttl`

## Check datacube
Skript kontroluje integritní omezení jednotlivých data cubes. Za argument dostavá cestu k datacube, který ověrí
Příklad spuštění skriptu
```commandline
python3 check_datacube.py "path/to/datacube"
```
