CREATE KEYSPACE IF NOT EXISTS it_equipment 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE it_equipment;

CREATE TABLE budynek (id INT PRIMARY KEY, adres TEXT, oznaczenie TEXT);
CREATE TABLE dzial (id INT PRIMARY KEY, nazwa TEXT);
CREATE TABLE karta (id INT PRIMARY KEY, numer TEXT);
CREATE TABLE producent (id INT PRIMARY KEY, nazwa TEXT);
CREATE TABLE przetarg (id INT PRIMARY KEY, numerumowy TEXT, datarozpoczecia TIMESTAMP, datazakonczenia TIMESTAMP);
CREATE TABLE lokalizacja (id INT PRIMARY KEY, pokoj INT, budynekid INT);
CREATE TABLE model (id INT PRIMARY KEY, nazwa TEXT, producentid INT);
CREATE TABLE pracownik (id INT PRIMARY KEY, imie TEXT, nazwisko TEXT, dzialid INT, kartaid INT);
CREATE TABLE urzadzenie (id INT PRIMARY KEY, nazwa TEXT, przetargid INT, numerseryjny TEXT, stan TEXT, modelid INT);
CREATE TABLE historiaoperacji (id INT PRIMARY KEY, urzadzenieid INT, pracownikid INT, lokalizacjaid INT, data_zdarzenia TIMESTAMP, log TEXT);