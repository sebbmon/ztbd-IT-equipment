import time
import psycopg2
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster

# conf + polaczenia
def connect_all():
    pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
    pg.autocommit = True
    
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    my.autocommit = True
    
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    
    cluster = Cluster(['localhost'], port=9042)
    cass = cluster.connect('it_equipment')
    cass.default_timeout = 60.0 
    
    return pg, my, mongo, cass

def my_execute(my_conn, sql):
    # wymuszenie buforowania i zjadania calego wyniku zeby mysql sie nie zadlawil
    cur = my_conn.cursor(buffered=True)
    try:
        cur.execute(sql)
        if cur.description is not None:
            cur.fetchall()
    finally:
        cur.close()

def measure(db_name, func):
    times = []
    for _ in range(3):
        try:
            start = time.time()
            func()
            times.append((time.time() - start) * 1000)
        except Exception as e:
            print(f"   [{db_name.ljust(9)}] BŁĄD: {str(e).strip().splitlines()[0]}")
            return
    avg = sum(times) / 3
    print(f"   [{db_name.ljust(9)}] Średni czas: {avg:8.2f} ms")

# 24 scenariusze full crud
def run_tests(pg, my, mongo, cass):
    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: CREATE")
    print("="*50)

    print("\n[C1] Dodanie nowego budynku (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1') ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1');"))
    measure("Mongo", lambda: mongo["budynek"].insert_one({"id": 999901, "adres": "Testowa 1", "oznaczenie": "B1"}) if not mongo["budynek"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1');"))

    print("\n[C2] Dodanie nowego działu (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy') ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy');"))
    measure("Mongo", lambda: mongo["dzial"].insert_one({"id": 999901, "nazwa": "Dzial Testowy"}) if not mongo["dzial"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy');"))

    print("\n[C3] Dodanie nowego producenta (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy') ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy');"))
    measure("Mongo", lambda: mongo["producent"].insert_one({"id": 999901, "nazwa": "Producent Testowy"}) if not mongo["producent"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy');"))

    print("\n[C4] Wydanie nowej karty dostępowej (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO karta (id, numer) VALUES (999901, 'KARTA-999') ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO karta (id, numer) VALUES (999901, 'KARTA-999');"))
    measure("Mongo", lambda: mongo["karta"].insert_one({"id": 999901, "numer": "KARTA-999"}) if not mongo["karta"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO karta (id, numer) VALUES (999901, 'KARTA-999');"))

    print("\n[C5] Zatrudnienie pracownika testowego (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901) ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901);"))
    measure("Mongo", lambda: mongo["pracownik"].insert_one({"id": 999901, "imie": "Jan", "nazwisko": "Testowy", "dzialid": 999901, "kartaid": 999901}) if not mongo["pracownik"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901);"))

    print("\n[C6] Wprowadzenie urządzenia testowego (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1) ON CONFLICT DO NOTHING;"))
    measure("MySQL", lambda: my_execute(my, "INSERT IGNORE INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1);"))
    measure("Mongo", lambda: mongo["urzadzenie"].insert_one({"id": 999901, "nazwa": "Urzadzenie-Test", "przetargid": 1, "numerseryjny": "SN-TEST-1", "stan": "Nowy", "modelid": 1}) if not mongo["urzadzenie"].find_one({"id": 999901}) else None)
    measure("Cassandra", lambda: cass.execute("INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1);"))

    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: READ")
    print("="*50)

    print("\n[R1] Wyszukanie urządzenia po ID = 100")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM urzadzenie WHERE id = 100;"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM urzadzenie WHERE id = 100;"))
    measure("Mongo", lambda: mongo["urzadzenie"].find_one({"id": 100}))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM urzadzenie WHERE id = 100;"))

    print("\n[R2] Wyszukanie urządzenia po zaindeksowanej nazwie ('Urzadzenie-50000')")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"))
    measure("Mongo", lambda: list(mongo["urzadzenie"].find({"nazwa": "Urzadzenie-50000"})))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"))

    print("\n[R3] Sprawdzenie danych pracownika po ID = 100")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM pracownik WHERE id = 100;"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM pracownik WHERE id = 100;"))
    measure("Mongo", lambda: mongo["pracownik"].find_one({"id": 100}))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM pracownik WHERE id = 100;"))

    print("\n[R4] Odczytanie szczegółów przetargu ID = 10")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM przetarg WHERE id = 10;"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM przetarg WHERE id = 10;"))
    measure("Mongo", lambda: mongo["przetarg"].find_one({"id": 10}))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM przetarg WHERE id = 10;"))

    print("\n[R5] Pobranie danych producenta ID = 5")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM producent WHERE id = 5;"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM producent WHERE id = 5;"))
    measure("Mongo", lambda: mongo["producent"].find_one({"id": 5}))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM producent WHERE id = 5;"))

    print("\n[R6] Odczyt lokalizacji ID = 10")
    measure("Postgres", lambda: pg.cursor().execute("SELECT * FROM lokalizacja WHERE id = 10;"))
    measure("MySQL", lambda: my_execute(my, "SELECT * FROM lokalizacja WHERE id = 10;"))
    measure("Mongo", lambda: mongo["lokalizacja"].find_one({"id": 10}))
    measure("Cassandra", lambda: cass.execute("SELECT * FROM lokalizacja WHERE id = 10;"))

    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: UPDATE")
    print("="*50)

    print("\n[U1] Zmiana statusu testowego urządzenia na 'W użyciu'")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["urzadzenie"].update_one({"id": 999901}, {"$set": {"stan": "W użyciu"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"))

    print("\n[U2] Aktualizacja adresu testowego budynku")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["budynek"].update_one({"id": 999901}, {"$set": {"adres": "Nowy Adres 2"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"))

    print("\n[U3] Zmiana nazwy testowego działu")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["dzial"].update_one({"id": 999901}, {"$set": {"nazwa": "Dzial Super Testowy"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"))

    print("\n[U4] Zmiana nazwiska testowego pracownika")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["pracownik"].update_one({"id": 999901}, {"$set": {"nazwisko": "Kowalski"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"))

    print("\n[U5] Aktualizacja nazwy producenta")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["producent"].update_one({"id": 999901}, {"$set": {"nazwa": "Nowy Producent"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"))

    print("\n[U6] Zmiana numeru seryjnego testowego urządzenia")
    measure("Postgres", lambda: pg.cursor().execute("UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["urzadzenie"].update_one({"id": 999901}, {"$set": {"numerseryjny": "SN-NEW-99"}}))
    measure("Cassandra", lambda: cass.execute("UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"))

    print("\n" + "="*50)
    print(" ROZPOCZYNAM TESTY: DELETE (Usuwanie)")
    print("="*50)

    #naprawa
    print("\n[Czyszczenie pomocnicze przed D1] Usuwanie historii powiązanej z urządzeniem")
    try: pg.cursor().execute("DELETE FROM historiaoperacji WHERE urzadzenieid = 999901;")
    except: pass
    try: my_execute(my, "DELETE FROM historiaoperacji WHERE urzadzenieid = 999901;")
    except: pass

    print("\n[D1] Złomowanie testowego urządzenia (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM urzadzenie WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM urzadzenie WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["urzadzenie"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM urzadzenie WHERE id = 999901;"))

    print("\n[D2] Usunięcie testowego pracownika (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM pracownik WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM pracownik WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["pracownik"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM pracownik WHERE id = 999901;"))

    print("\n[D3] Usunięcie testowej karty dostępowej (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM karta WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM karta WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["karta"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM karta WHERE id = 999901;"))

    print("\n[D4] Usunięcie testowego producenta (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM producent WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM producent WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["producent"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM producent WHERE id = 999901;"))

    print("\n[D5] Usunięcie testowego działu (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM dzial WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM dzial WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["dzial"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM dzial WHERE id = 999901;"))

    print("\n[D6] Wyburzenie testowego budynku (ID: 999901)")
    measure("Postgres", lambda: pg.cursor().execute("DELETE FROM budynek WHERE id = 999901;"))
    measure("MySQL", lambda: my_execute(my, "DELETE FROM budynek WHERE id = 999901;"))
    measure("Mongo", lambda: mongo["budynek"].delete_one({"id": 999901}))
    measure("Cassandra", lambda: cass.execute("DELETE FROM budynek WHERE id = 999901;"))

# glowna petla
if __name__ == "__main__":
    print("Nawiązywanie połączeń z bazami...")
    pg, my, mongo, cass = connect_all()
    print("Połączenia nawiązane poprawnie!")
    
    run_tests(pg, my, mongo, cass)
    
    pg.close()
    my.close()
    cass.cluster.shutdown()
    print("\n==================================================")
    print(" TESTY ZAKOŃCZONE")
    print("==================================================")