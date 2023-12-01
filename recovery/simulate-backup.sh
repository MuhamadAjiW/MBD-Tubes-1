#!/bin/bash

set -e # exit on error

DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432

POSTGRES="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/postgres"
DELLSTORE="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/dellstore"

docker compose down 2>/dev/null

sudo echo "[*] Setting up folders"
# setup folders
sudo rm -rf data
mkdir data

sudo rm -rf backup
mkdir -p backup
sudo chown 999 backup

sudo rm -rf archive
mkdir -p archive
sudo chown 999 archive

echo "[*] Enabling continuous archiving"
docker compose up -d 2>/dev/null
while ! docker compose exec db pg_isready -U postgres >/dev/null; do sleep 1; done # pastikan postgresql sudah siap menerima request

# enable continuous archiving
sudo sed -i 's/#archive_mode = off/archive_mode = on/' data/postgresql.conf
sudo sed -i "s/#archive_command = ''/archive_command = 'test ! -f \/archive\/%f \&\& cp %p \/archive\/%f'/" data/postgresql.conf
sudo sed -i "s/#wal_level/wal_level/" data/postgresql.conf

echo "[*] Preparing base backup"
# base backup
docker compose down -v 2>/dev/null
docker compose up -d 2>/dev/null
while ! docker compose exec db pg_isready -U postgres >/dev/null; do sleep 1; done
docker compose exec --user postgres db pg_basebackup -D /backup >/dev/null

echo "[*] Creating and populating database"
psql $POSTGRES -c "create database dellstore" >/dev/null
psql $DELLSTORE <dellstore2-normal-1.0.sql >/dev/null
TIME=$(date +"%Y-%m-%d %H:%M:%S%z")

echo "[~] Before failure"
psql $POSTGRES -c "\l"

echo "[*] Simulating failure (deleting database)"
# di sini database dellstore akan dihapus dan tujuan saya nantinya adalah merestore database sebelum dihapus
psql $POSTGRES -c "drop database dellstore" >/dev/null

echo "[~] After failure"
psql $POSTGRES -c "\l"

echo "[*] Restoring database using PITR method"
# PITR
docker compose down -v 2>/dev/null
sudo rm -rf data/*
sudo cp -a backup/. data/
sudo sed -i "s/#restore_command = ''/restore_command = 'cp \/archive\/%f %p'/" data/postgresql.conf
sudo sed -i "s/#recovery_target_time = ''/recovery_target_time = '$TIME'/" data/postgresql.conf
sudo touch data/recovery.signal
docker compose up -d 2>/dev/null
(
	while ! docker compose exec db pg_isready -U postgres >/dev/null; do sleep 1; done
	# postgresql akan mempause proses recovery
	# command ini diperlukan untuk melanjutkan proses recovery
	psql $POSTGRES -c "select pg_wal_replay_resume()" >/dev/null
) &

echo "[~] After recovery"
docker compose up -d 2>/dev/null
while ! docker compose exec db pg_isready -U postgres >/dev/null; do sleep 1; done
psql $POSTGRES -c "\l"
docker compose down -v 2>/dev/null

# cleanup (jaga-jaga doang)
sudo rm -rf data/recovery.signal

# Testing apakah recovery benar-benar berhasil
docker compose up -d 2>/dev/null
while ! docker compose exec db pg_isready -U postgres >/dev/null; do sleep 1; done
(psql $DELLSTORE -c "select * from categories limit 1" &>/dev/null && echo "[+] Successfully recovered") || echo "[-] Recovery failed"
docker compose down 2>/dev/null
