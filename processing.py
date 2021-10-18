import xmltodict
import collections
import psycopg2
import glob
import json

def guaranteed_list(x):
    if not x:
        return []
    elif isinstance(x, list):
        return x
    else:
        return [x]

try:
    conn = psycopg2.connect("dbname='osm' user='osm' host='localhost' password='osm'")
except:
    print("Blad polaczenia z baza")
cur = conn.cursor()

for filepath in glob.iglob('c:/ekw/*.xml'):
    with open(filepath) as fd:
       	doc = xmltodict.parse(fd.read())
    """ Przygotuj nowy numer księgi oraz opis stanu księgi (rubryka 0.2) """
    wydzial = doc['KW']['R01']['P1']['@Wk']
    repertorium = doc['KW']['R01']['P1']['@Nr']
    cyfrakontroli = doc['KW']['R01']['P1']['@Ck']
    kw = wydzial + '/'+ repertorium + '/' + cyfrakontroli
    """ Przygotuj opis stanu księgi """
    stankw = doc['KW']['R02']['P1']['@Tr']
    zapisaniekw = doc['KW']['R02']['P2']['@Tr']
    ujawnieniekw = doc['KW']['R02']['P3']['@Tr']
    try:
        dotychczasowakw = doc['KW']['R02']['P4']['@Tr']
    except:
        dotychczasowakw = ' '
#Zapisz do bazy numer i stan księgi
    #cur.execute("INSERT INTO ekw.r02 VALUES(%s, %s, %s, %s, %s)", (kw, stankw, zapisaniekw, ujawnieniekw, dotychczasowakw))

    """ Przygotuj rubrykę 1.3 w dziale I-O położenie nieruchomości - uwaga, jeszcze nie dziala"""
    opispolozenia= collections.OrderedDict()
    for polozenie in guaranteed_list(doc['KW']['D1o']['R13']['E']):
        opispolozenia['kw'] = kw
        try:
            opispolozenia['lp'] = polozenie['P1']['@Tr']
        except:
            opispolozenia['lp'] = ' '
        try:
            opispolozenia['wojewodztwo'] = polozenie['P2']['@Tr']
        except:
            opispolozenia['wojewodztwo'] = ' '
        try:
            opispolozenia['powiat'] = polozenie['P3']['@Tr']
        except:
            opispolozenia['powiat'] = ' '
        try:
            opispolozenia['gmina'] = polozenie['P4']['@Tr']
        except:
            opispolozenia['gmina'] = ' '
        try:
            opispolozenia['miejscowosc'] = polozenie['P5']['@Tr']
        except:
            opispolozenia['miejscowosc'] = ' '
        #cur.execute("INSERT INTO ekw.d1or13 VALUES(%s, %s, %s, %s, %s, %s)", (list(opispolozenia.values())))

    """ Przygotuj rubrykę 1.4 podrubrykę 1.4.1 oznaczenie działki  """
    opisdzialki= collections.OrderedDict()
    for oznaczenie in guaranteed_list(doc['KW']['D1o']['R14']['PR141']['E']):
        opisdzialki['kw'] = kw
        try:
            opisdzialki['iddzialki'] = oznaczenie['P1']['@Tr']
        except:
            opisdzialki['iddzialki'] = ' '
        try:
            opisdzialki['numerdzialki'] = oznaczenie['P2']['@Tr']
        except:
            opisdzialki['numerdzialki'] = ' '
        try:
            opisdzialki['sposobko'] = oznaczenie['P6']['@Tr']
        except:
            opisdzialki['sposobko'] = ' '
        try:
            opisdzialki['przylaczenie'] = oznaczenie['P7']['A']['@Wk'] + '/' + oznaczenie['P7']['A']['@Nr'] + '/' + oznaczenie['P7']['A']['@Ck']
        except:
            opisdzialki['przylaczenie'] = ' '
        #cur.execute("INSERT INTO ekw.d1or14 VALUES(%s, %s, %s, %s, %s)", (list(opisdzialki.values())))

    """ Przygotuj dane dla rubryki R2.2 księgi """
    try:
        opiswlasciela = collections.OrderedDict()
        for skarb in doc['KW']['D2']['R22']['PR2']['E']['SP']['I']['N']:
            opiswlasciela['kw'] = kw
            opiswlasciela['wlasnosc'] = skarb['@Tr']
            opiswlasciela['wlasciciel'] = 'sp'
            print(kw + " - Skarb Państwa")
            cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (list(opiswlasciciela.values())))
    except:
       try:
           zarzadnazwisko = doc['KW']['D2']['R22']['PR5']['E']['OF']['N1']['@Tr']
           zarzadimie = doc['KW']['D2']['R22']['PR5']['E']['OF']['I1']['@Tr']
           wlasnosc = zarzadnazwisko + ' ' + zarzadimie
           wlasciciel = 'of'
           #TODO: Dodaj wiele wierszy nazwy osoby fizycznej N1
           print(kw + " - Osoba fizyczna")
           cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (kw, wlasnosc, wlasciciel))
       except:
           print(kw + " - Nie odnalazłem poprawnie rubryk w R2.2")
conn.commit()
cur.close()
conn.close()
