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
        dotychczasowakw = 'brak'
#Zapisz do bazy numer i stan księgi
    cur.execute("INSERT INTO ekw.r02 VALUES(%s, %s, %s, %s, %s)", (kw, stankw, zapisaniekw, ujawnieniekw, dotychczasowakw))

    """ Przygotuj rubrykę 1.3 w dziale I-O położenie nieruchomości"""
    opispolozenia= collections.OrderedDict()
    for polozenie in guaranteed_list(doc['KW']['D1o']['R13']['E']):
        opispolozenia['kw'] = kw
        try:
            opispolozenia['lp'] = polozenie['P1']['@Tr']
        except:
            opispolozenia['lp'] = 'XX'
        try:
            opispolozenia['wojewodztwo'] = polozenie['P2']['@Tr']
        except:
            opispolozenia['wojewodztwo'] = 'XX'
        try:
            opispolozenia['powiat'] = polozenie['P3']['@Tr']
        except:
            opispolozenia['powiat'] = 'XX'
        try:
            opispolozenia['gmina'] = polozenie['P4']['@Tr']
        except:
            opispolozenia['gmina'] = 'XX'
        try:
            opispolozenia['miejscowosc'] = polozenie['P5']['@Tr']
        except:
            opispolozenia['miejscowosc'] = 'XX'
        cur.execute("INSERT INTO ekw.d1or13 VALUES(%s, %s, %s, %s, %s, %s)", (list(opispolozenia.values())))

    """ Przygotuj rubrykę 1.4 podrubrykę 1.4.1 oznaczenie działki  """
    opisdzialki= collections.OrderedDict()
    for oznaczenie in guaranteed_list(doc['KW']['D1o']['R14']['PR141']['E']):
        opisdzialki['kw'] = kw
        try:
            opisdzialki['iddzialki'] = oznaczenie['P1']['@Tr']
        except:
            opisdzialki['iddzialki'] = 'XX'
        try:
            nrdzialki = collections.OrderedDict()
            for nr in guaranteed_list(oznaczenie['P2']['@Tr']):
                nrdzialki['dzew'] = nr
            opisdzialki['numerdzialki'] = nrdzialki['dzew']
        except:
            opisdzialki['numerdzialki'] = 'nie wpisałem poprawnego - sprawdź'
        try:
            opisdzialki['sposobko'] = oznaczenie['P6']['@Tr']
        except:
            opisdzialki['sposobko'] = 'XX'
        try:
            opisdzialki['przylaczenie'] = oznaczenie['P7']['A']['@Wk'] + '/' + oznaczenie['P7']['A']['@Nr'] + '/' + oznaczenie['P7']['A']['@Ck']
        except:
            opisdzialki['przylaczenie'] = 'XX'
        try:
            opisdzialki['lpn'] = oznaczenie['P4']['E']['@Tr']
        except:
            opisdzialki['lpn'] = 'nie wpisałem poprawnego - sprawdź'
        cur.execute("INSERT INTO ekw.d1or14 VALUES(%s, %s, %s, %s, %s, %s)", (list(opisdzialki.values())))

    """ Przygotuj dane dla rubryki R2.2 księgi """
    try:
# Tylko jedna rubryka PR2 E - szukamy w instytucjach jednego właściciela
        opiswlasciciela = collections.OrderedDict()
        opiswlasciciela['kw'] = kw
        opiswlasciciela['zarzad'] = doc['KW']['D2']['R22']['PR2']['E']['SP']['I']['N']['@Tr']
        opiswlasciciela['wlasciciel'] = 'sp - więcej wpisów'
        cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (list(opiswlasciciela.values())))
    except:
        try:
# Szukamy dwóch i więcej właścieili SP
            opiswlasciciela = collections.OrderedDict()
            for skarb in doc['KW']['D2']['R22']['PR2']['E']['SP']['I']['N']:
                opiswlasciciela['kw'] = kw
                opiswlasciciela['zarzad'] = skarb['@Tr']
                opiswlasciciela['wlasciciel'] = 'sp - więcej wpisów'
                cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (list(opiswlasciciela.values())))
        except:
            try:
# Wiele rubryk PR2 E - szukamy w instytucjach
                opiswlasciciela = collections.OrderedDict()
                for skarb in doc['KW']['D2']['R22']['PR2']['E']:
                    opiswlasciciela['kw'] = kw
                    opiswlasciciela['zarzad'] = skarb['SP']['I']['N']['@Tr']
                    opiswlasciciela['wlasciciel'] = 'sp - wpisy w wielu rubrykach'
                    cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (list(opiswlasciciela.values())))
            except:
                try:
# Rubryka PR3 E - szukamy w instytucjach JST
                    opiswlasciciela = collections.OrderedDict()
                    for skarb in doc['KW']['D2']['R22']['PR3']['E']['JT']['I']['N']:
                        opiswlasciciela['kw'] = kw
                        opiswlasciciela['zarzad'] = skarb['@Tr']
                        opiswlasciciela['wlasciciel'] = 'jst'
                        cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (list(opiswlasciciela.values())))
                except:
                    try:
# Runryka PR5 - szukamy osób fizycznych
                        zarzadnazwisko = doc['KW']['D2']['R22']['PR5']['E']['OF']['N1']['@Tr']
                        wlasnosc = 'NN - osoba fizyczna'
                        wlasciciel = 'of'
                        cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s)", (kw, wlasnosc, wlasciciel))
                    except:
                        print(kw + " - Nie odnalazłem poprawnie rubryk w R2.2")
conn.commit()
cur.close()
conn.close()
