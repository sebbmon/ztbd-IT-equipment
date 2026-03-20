import csv
import random
import os
import time
from faker import Faker
from datetime import datetime, timedelta

# polski faker
fake = Faker('pl_PL')

if not os.path.exists('data'):
    os.makedirs('data')

# konfiguracja wielkosci zestawow
NUM_BUDYNKI = 3
NUM_DZIALY = 3
NUM_PRODUCENCI = 3
NUM_PRZETARGI = 25
NUM_MODELE = 100
NUM_LOKALIZACJE = 250
NUM_KARTY = 250
NUM_PRACOWNICY = 250

NUM_URZADZENIA = 150000
NUM_HISTORIA = 344618

# pomocnicze
def save_to_csv(filename, headers, data):
    filepath = os.path.join('data', filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"[OK] Wygenerowano {filepath} ({len(data)} rekordów)")

# tablice dla masowego generatora
stany = ['Nowy', 'W użyciu', 'W naprawie', 'Zmagazynowany']
logi = ['Wydanie pracownikowi', 'Zwrot do magazynu', 'Przekazanie do serwisu', 'Inwentaryzacja']

# generowanie malych tabel
def generate_dictionaries():
    print("--- Generowanie tabel słownikowych ---")
    
    # 1. BUDYNKI
    budynki = [[i, fake.address().replace('\n', ', '), f"B{i}"] for i in range(1, NUM_BUDYNKI + 1)]
    save_to_csv('budynek.csv', ['id', 'adres', 'oznaczenie'], budynki)

    # 2. LOKALIZACJE 
    lokalizacje = [[i, random.randint(100, 999), random.randint(1, NUM_BUDYNKI)] for i in range(1, NUM_LOKALIZACJE + 1)]
    save_to_csv('lokalizacja.csv', ['id', 'pokoj', 'budynekid'], lokalizacje)

    # 3. DZIAŁY
    dzialy_nazwy = ['IT', 'HR', 'Księgowość', 'Marketing', 'Sprzedaż', 'Logistyka', 'Zarząd', 'Obsługa Klienta', 'R&D', 'Prawny']
    dzialy = [[i, dzialy_nazwy[(i-1) % len(dzialy_nazwy)]] for i in range(1, NUM_DZIALY + 1)]
    save_to_csv('dzial.csv', ['id', 'nazwa'], dzialy)

    # 4. KARTY
    karty = [[i, fake.hexify(text='^^:^^:^^:^^:^^', upper=True)] for i in range(1, NUM_KARTY + 1)]
    save_to_csv('karta.csv', ['id', 'numer'], karty)

    # 5. PRACOWNICY
    pracownicy = [[i, fake.first_name(), fake.last_name(), random.randint(1, NUM_DZIALY), i] for i in range(1, NUM_PRACOWNICY + 1)]
    save_to_csv('pracownik.csv', ['id', 'imie', 'nazwisko', 'dzialid', 'kartaid'], pracownicy)

    # 6. PRZETARGI
    przetargi = []
    for i in range(1, NUM_PRZETARGI + 1):
        start = fake.date_time_between(start_date='-5y', end_date='-1y')
        end = start + timedelta(days=random.randint(30, 365))
        przetargi.append([i, f"ZAM/{fake.year()}/{random.randint(100,999)}", start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')])
    save_to_csv('przetarg.csv', ['id', 'numerumowy', 'datarozpoczecia', 'datazakonczenia'], przetargi)

    # 7. PRODUCENCI
    producenci_nazwy = ['Dell', 'HP', 'Lenovo', 'Apple', 'Asus', 'Acer', 'Brother', 'Cisco', 'Samsung', 'LG']
    producenci = [[i, producenci_nazwy[(i-1) % len(producenci_nazwy)]] for i in range(1, NUM_PRODUCENCI + 1)]
    save_to_csv('producent.csv', ['id', 'nazwa'], producenci)

    # 8. MODELE
    modele = [[i, f"Model-{fake.word().capitalize()}-{random.randint(1000, 9999)}", random.randint(1, NUM_PRODUCENCI)] for i in range(1, NUM_MODELE + 1)]
    save_to_csv('model.csv', ['id', 'nazwa', 'producentid'], modele)

# generowanie duzych tabel
def generate_massive_urzadzenia():
    print("\n--- Generowanie urządzeń ---")
    start_time = time.time()
    
    with open('data/urzadzenie.csv', mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'nazwa', 'przetargid', 'numerseryjny', 'stan', 'modelid'])
        
        for i in range(1, NUM_URZADZENIA + 1):
            sn = f"SN{random.randint(100000, 999999)}X{i}"
            writer.writerow([
                i, 
                f"Urzadzenie-{i}", 
                random.randint(1, NUM_PRZETARGI), 
                sn, 
                random.choice(stany), 
                random.randint(1, NUM_MODELE)
            ])
            
            if i % 50000 == 0:
                print(f" -> Zapisano {i} / {NUM_URZADZENIA} urządzeń...")
                
    print(f"[OK] Ukończono urządzenia w {round(time.time() - start_time, 2)} sekund!")

def generate_massive_historia():
    print("\n--- Generowanie historii operacji ---")
    start_time = time.time()
    
    with open('data/historiaoperacji.csv', mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'urzadzenieid', 'pracownikid', 'lokalizacjaid', 'data_zdarzenia', 'log'])
        
        for i in range(1, NUM_HISTORIA + 1):
            rok = random.randint(2020, 2024)
            miesiac = random.randint(1, 12)
            dzien = random.randint(1, 28)
            godzina = random.randint(8, 16)
            data_str = f"{rok}-{miesiac:02d}-{dzien:02d} {godzina:02d}:00:00"
            
            writer.writerow([
                i,
                random.randint(1, NUM_URZADZENIA),
                random.randint(1, NUM_PRACOWNICY),
                random.randint(1, NUM_LOKALIZACJE),
                data_str,
                random.choice(logi)
            ])
            
            if i % 100000 == 0:
                print(f" -> Zapisano {i} / {NUM_HISTORIA} operacji...")
                
    print(f"[OK] Ukończono historię w {round(time.time() - start_time, 2)} sekund")

if __name__ == "__main__":
    print("======================================================")
    print(" ROZPOCZYNAM GENEROWANIE DANYCH (ZESTAW ZINTEGROWANY)")
    print("======================================================")
    
    generate_dictionaries()
    generate_massive_urzadzenia()
    generate_massive_historia()
    
    print("\n======================================================")
    print(" WYGENEROWANO WSZYSTKIE PLIKI CSV")
    print("======================================================")