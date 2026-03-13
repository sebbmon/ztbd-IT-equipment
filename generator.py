import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# polski generator
fake = Faker('pl_PL')

import os
if not os.path.exists('data'):
    os.makedirs('data')

# parametry dla małych tabel
NUM_BUDYNKI = 50
NUM_DZIALY = 50
NUM_PRODUCENCI = 50
NUM_PRZETARGI = 500
NUM_MODELE = 2000
NUM_LOKALIZACJE = 5000
NUM_KARTY = 50000
NUM_PRACOWNICY = 50000
# NUM_URZADZENIA = 200
# NUM_HISTORIA = 500

def save_to_csv(filename, headers, data):
    filepath = os.path.join('data', filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"Wygenerowano {filepath} ({len(data)} rekordów)")

print("Rozpoczynam generowanie danych...")

# 1. BUDYNKI
budynki = [[i, fake.address().replace('\n', ', '), f"B{i}"] for i in range(1, NUM_BUDYNKI + 1)]
save_to_csv('budynek.csv', ['id', 'adres', 'oznaczenie'], budynki)

# 2. LOKALIZACJE 
lokalizacje = [[i, random.randint(100, 999), random.randint(1, NUM_BUDYNKI)] for i in range(1, NUM_LOKALIZACJE + 1)]
save_to_csv('lokalizacja.csv', ['id', 'pokoj', 'budynekid'], lokalizacje)

# 3. DZIAŁY
dzialy_nazwy = ['IT', 'HR', 'Księgowość', 'Marketing', 'Sprzedaż', 'Logistyka', 'Zarząd', 'Obsługa Klienta', 'R&D', 'Prawny']
dzialy = [[i, dzialy_nazwy[i-1]] for i in range(1, len(dzialy_nazwy) + 1)]
save_to_csv('dzial.csv', ['id', 'nazwa'], dzialy)

# 4. KARTY
karty = [[i, fake.hexify(text='^^:^^:^^:^^:^^', upper=True)] for i in range(1, NUM_KARTY + 1)]
save_to_csv('karta.csv', ['id', 'numer'], karty)

# 5. PRACOWNICY
pracownicy = [[i, fake.first_name(), fake.last_name(), random.randint(1, len(dzialy)), i] for i in range(1, NUM_PRACOWNICY + 1)]
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
producenci = [[i, producenci_nazwy[i-1]] for i in range(1, len(producenci_nazwy) + 1)]
save_to_csv('producent.csv', ['id', 'nazwa'], producenci)

# 8. MODELE
modele = [[i, f"Model-{fake.word().capitalize()}-{random.randint(1000, 9999)}", random.randint(1, len(producenci))] for i in range(1, NUM_MODELE + 1)]
save_to_csv('model.csv', ['id', 'nazwa', 'producentid'], modele)

"""
# 9. URZĄDZENIA
stany = ['Nowy', 'W użyciu', 'W naprawie', 'Zmagazynowany']
urzadzenia = []
for i in range(1, NUM_URZADZENIA + 1):
    urzadzenia.append([
        i, 
        f"Urzadzenie-{i}", 
        random.randint(1, NUM_PRZETARGI), 
        fake.ean(length=13), # Numer seryjny
        random.choice(stany), 
        random.randint(1, NUM_MODELE)
    ])
save_to_csv('urzadzenie.csv', ['id', 'nazwa', 'przetargid', 'numerseryjny', 'stan', 'modelid'], urzadzenia)

# 10. HISTORIA OPERACJI
logi = ['Wydanie pracownikowi', 'Zwrot do magazynu', 'Przekazanie do serwisu', 'Inwentaryzacja']
historia = []
for i in range(1, NUM_HISTORIA + 1):
    data_zd = fake.date_time_between(start_date='-2y', end_date='now')
    historia.append([
        i,
        random.randint(1, NUM_URZADZENIA),
        random.randint(1, NUM_PRACOWNICY),
        random.randint(1, NUM_LOKALIZACJE),
        data_zd.strftime('%Y-%m-%d %H:%M:%S'),
        random.choice(logi)
    ])
save_to_csv('historiaoperacji.csv', ['id', 'urzadzenieid', 'pracownikid', 'lokalizacjaid', 'data_zdarzenia', 'log'], historia)
"""

print("Gotowe")