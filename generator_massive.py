import csv
import random
import os
import time

if not os.path.exists('data'):
    os.makedirs('data')

# parametry, suma = 10m
NUM_BUDYNKI = 50
NUM_DZIALY = 50
NUM_PRODUCENCI = 50
NUM_PRZETARGI = 500
NUM_MODELE = 2000
NUM_LOKALIZACJE = 5000
NUM_KARTY = 50000
NUM_PRACOWNICY = 50000
NUM_URZADZENIA = 3000000
NUM_HISTORIA = 6892350

stany = ['Nowy', 'W uzyciu', 'W naprawie', 'Zmagazynowany']
logi = ['Wydanie pracownikowi', 'Zwrot do magazynu', 'Przekazanie do serwisu', 'Inwentaryzacja']

def generate_massive_urzadzenia():
    print("generowanie 3 milionow urzadzen...")
    start_time = time.time()
    
    with open('data/urzadzenie.csv', mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'nazwa', 'przetargid', 'numerseryjny', 'stan', 'modelid'])
        
        for i in range(1, NUM_URZADZENIA + 1):
            # szybsze random zamiast fakera
            sn = f"SN{random.randint(100000, 999999)}X{i}"
            writer.writerow([
                i, 
                f"Urzadzenie-{i}", 
                random.randint(1, NUM_PRZETARGI), 
                sn, 
                random.choice(stany), 
                random.randint(1, NUM_MODELE)
            ])
            
            # druk postepu co 500k
            if i % 500000 == 0:
                print(f" -> Zapisano {i} / {NUM_URZADZENIA} urządzeń...")
                
    print(f"Ukończono urządzenia w {round(time.time() - start_time, 2)} sekund!\n")

def generate_massive_historia():
    print("Generowanie 7 milionow operacji...")
    start_time = time.time()
    
    with open('data/historiaoperacji.csv', mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'urzadzenieid', 'pracownikid', 'lokalizacjaid', 'data_zdarzenia', 'log'])
        
        for i in range(1, NUM_HISTORIA + 1):
            # szybsze generowanie daty jako stringa
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
            
            if i % 1000000 == 0:
                print(f" -> Zapisano {i} / {NUM_HISTORIA} operacji...")
                
    print(f"Ukończono historię w {round(time.time() - start_time, 2)} sekund\n")

generate_massive_urzadzenia()
generate_massive_historia()

print("Gotowe")