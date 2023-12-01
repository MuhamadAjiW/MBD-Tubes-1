# Mekanisme Concurrency Control dan Recovery Basis Data

> Disusun untuk memenuhi Tugas Besar IF3140 Manajemen Basis Data

## Daftar Isi

- [Deskripsi Aplikasi](#deskripsi-aplikasi)
- [Daftar _Requirement_](#daftar-requirement)
- [Cara Menjalankan](#cara-menjalankan)
- [Pembagian Tugas](#pembagian-tugas)

## Deskripsi Aplikasi

Tugas ini terdiri dari beberapa bagian yang diperlukan untuk memenuhi spesifikasi Tugas Besar IF3140 Manajemen Basis Data. Pada spesifikasi tugas tersebut, penulis diminta untuk melakukan eksplorasi terhadap DBMS yaitu PostgreSQL, khususnya pada bagian concurrency control, locking, dan recovery. Setiap folder berisikan aplikasi yang diperlukan untuk eksplorasi tersebut. 

## Daftar requirement

- `Python` versi 3.7 ke atas
- Untuk recovery membutuhkan Linux OS atau WSL, `docker`, `docker-compose`, `postgresql`, dan `bash`

## Cara Menjalankan

### Two-Phase Locking

- cd ke two_phase_locking
- Jalankan perintah `python3 main.py`
- sesuaikan variabel filename dengan path input pada folder test

### Optimistic Concurrency Control (OCC)

- cd ke occ/src
- Jalankan perintah `python main.py`. Untuk mencoba file lain, buka main.py, ganti filename pada path.

### Multiversion Timestamp Ordering Concurrecy Control (MVCC)

- cd ke mvcc
- Jalankan perintah `py src/main.py tc/tc3.txt`. Sesuaikan "tc3.txt" dengan test case yang ingin digunakan
### Eksplorasi Recovery

Masuk ke direktori recovery lalu jalankan simulate-backup.sh (bash)

    > cd recovery
    > chmod +x ./simulate-backup.sh
    > ./simulate-backup.sh

## Pembagian tugas

1. Eksplorasi Transaction Isolation: 13521095
2. Two-Phase Locking: 13520137
3. Optimistic Concurrency Control: 13521063
4. Multiversion Timestamp Ordering Concurrecy Control: 13521151
5. Eksplorasi Recovery: 13521083
