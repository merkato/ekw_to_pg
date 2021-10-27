import xmltodict
import collections
import psycopg2
import datetime as dt
import glob
import json


def guaranteed_list(x):
    if not x:
        return []
    elif isinstance(x, list):
        return x
    else:
        return [x]

def numerksiegi(pole1):
    """ Przygotuj nowy numer księgi oraz opis stanu księgi (rubryka 0.2) """
    wydzial = pole1['@Wk']
    repertorium = pole1['@Nr']
    cyfrakontroli = pole1['@Ck']
    numerkw = wydzial + '/'+ repertorium + '/' + cyfrakontroli
    return numerkw

def stanksiegi(pole2):
    opisksiegi= collections.OrderedDict()
    """ Przygotuj opis stanu księgi """
    opisksiegi['kw'] = numerksiegi(doc['KW']['R01']['P1'])
    opisksiegi['stankw'] = pole2['P1']['@Tr']
    opisksiegi['zapisaniekw'] = pole2['P2']['@Tr']
    opisksiegi['ujawnieniekw'] = pole2['P3']['@Tr']
    try:
        opisksiegi['dotychczasowakw'] = pole2['P4']['@Tr']
    except:
        opisksiegi['dotychczasowakw'] = 'brak'
    opisksiegi['znacznikczasu'] = dt.datetime.now()
#Zapisz do bazy numer i stan księgi
    return opisksiegi

def polozenie(d1or13):
    """ Przygotuj rubrykę 1.3 w dziale I-O położenie nieruchomości"""
    opispolozenia = collections.OrderedDict()
    for polozenie in guaranteed_list(d1or13['E']):
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
        opispolozenia['znacznikczasu'] = dt.datetime.now()
    return opispolozenia

def dzialka(d1or14):
    """ Przygotuj rubrykę 1.4 podrubrykę 1.4.1 oznaczenie działki  """
    opisdzialki= collections.OrderedDict()
    for oznaczenie in guaranteed_list(d1or14['E']):
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
        opisdzialki['znacznikczasu'] = dt.datetime.now()
    return opisdzialki

def wlasciciel(d2r22):
    """ Przygotuj dane dla rubryki R2.2 księgi """
    opiswlasciciela = collections.OrderedDict()
    if d2r22['PR2'] is not None:
        for skarb in d2r22['PR2']['E']:
            opiswlasciciela['kw'] = kw
            opiswlasciciela['zarzad'] = skarb['SP']['I']['N']['@Tr']
            opiswlasciciela['wlasciciel'] = 'sp - wpisy w wielu rubrykach'
            try:
                opiswlasciciela['wpisi'] = skarb['SP']['I']['N']['I']['#text']
            except:
                opiswlasciciela['wpisi'] = 'nie znalazłem podstawy wpisu'
            try:
                opiswlasciciela['wpisd'] = skarb['SP']['I']['N']['D']['#text']
            except:
                opiswlasciciela['wpisd'] = 'nie znalazłem podstawy wykreślenia'
            cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s, %s, %s, %s)", (list(opiswlasciciela.values())))
    elif d2r22['PR3'] is not None:
        for skarb in d2r22['PR3']['E']['JT']['I']['N']:
            opiswlasciciela['kw'] = kw
            opiswlasciciela['zarzad'] = skarb['@Tr']
            opiswlasciciela['wlasciciel'] = 'jst'
            try:
                opiswlasciciela['wpisi'] = skarb['SP']['I']['N']['I']['#text']
            except:
                opiswlasciciela['wpisi'] = 'nie znalazłem podstawy wpisu'
            try:
                opiswlasciciela['wpisd'] = skarb['SP']['I']['N']['D']['#text']
            except:
                opiswlasciciela['wpisd'] = 'nie znalazłem podstawy wykreślenia'
            cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s, %s, %s)", (list(opiswlasciciela.values())))
    else:
            zarzadnazwisko = d2r22['PR5']['E']['OF']['N1']['@Tr']
            opiswlasciciela['kw'] = kw
            opiswlasciciela['zarzad'] = 'RODO - osoba fizyczna'
            opiswlasciciela['wlasciciel'] = 'of'
            try:
                opiswlasciciela['wpisi'] = d2r22['PR5']['E']['OF']['N1']['I']['#text']
            except:
                opiswlasciciela['wpisi'] = 'nie znalazłem podstawy wpisu'
            try:
                opiswlasciciela['wpisd'] = d2r22['PR5']['E']['OF']['N1']['D']['#text']
            except:
                opiswlasciciela['wpisd'] = 'nie znalazłem podstawy wykreślenia'
            cur.execute("INSERT INTO ekw.d2r22 VALUES(%s, %s, %s, %s, %s)", (list(opiswlasciciela.values())))
    return

try:
    conn = psycopg2.connect("dbname='osm' user='osm' host='localhost' password='osm'")
except:
    print("Blad polaczenia z baza")
cur = conn.cursor()

for filepath in glob.iglob('c:/ekw/*.xml'):
    with open(filepath) as fd:
        doc = xmltodict.parse(fd.read())
        kw = numerksiegi(doc['KW']['R01']['P1'])
        opisksiegi = stanksiegi(doc['KW']['R02'])
        cur.execute("INSERT INTO ekw.r02 VALUES(%s, %s, %s, %s, %s, %s)", (list(opisksiegi.values())))
        opispolozenia = polozenie(doc['KW']['D1o']['R13'])
        cur.execute("INSERT INTO ekw.d1or13 VALUES(%s, %s, %s, %s, %s, %s, %s)", (list(opispolozenia.values())))
        opisdzialki = dzialka(doc['KW']['D1o']['R14']['PR141'])
        cur.execute("INSERT INTO ekw.d1or14 VALUES(%s, %s, %s, %s, %s, %s, %s)", (list(opisdzialki.values())))
        wlasciciel(doc['KW']['D2']['R22'])

conn.commit()
cur.close()
conn.close()
