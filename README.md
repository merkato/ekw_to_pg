# EKW processing

Przetwarzanie XML z Ksiąg dla LP

## Przygotowanie

### Python3
1. Zainstaluj pakiet OSGeo4W - https://qgis.org/en/site/forusers/download.html
2. Wybierz tryb instalacji Advanced, a następnie wskaż do instalacji co najmniej pakiet python3-pip z zakładki Libs
3. Po zainstalowaniu, uruchom OSGeo4W Shell
4. Wywołaj komendę pip3 install xmltodict psycopg2

### Postgresql

W skrypcie ustaw dane dostępowe do bazy (w linii zaczynającej się od conn = psycopg2.connect), a następnie ścieżkę dostępu do katalogu z plikami EKW (for filepath in glob.iglob)

Wywołaj skrypt SQL ddl.sql (np. przy pomocy dBeaver'a)

## Uruchomienie

1. Uruchom OSGeo4W shell, zmień katalog bieżący na lokalizację skryptu.
2. Wywołaj polecenie python3 processing.py
