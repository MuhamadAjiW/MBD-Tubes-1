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
- Untuk recovery membutuhkan `docker` dan `bash`

## Cara Menjalankan

### Two-Phase Locking

> Lorem ipsum

### Optimistic Concurrency Control (OCC)

> Lorem ipsum

### Multiversion Timestamp Ordering Concurrecy Control (MVCC)

> Lorem ipsum

### Eksplorasi Recovery

Masuk ke direktori recovery lalu jalankan simulate-backup.sh (bash)

    > cd recovery
    > ./simulate-backup.sh

## Pembagian tugas

1. Eksplorasi Transaction Isolation: 13521095
2. Two-Phase Locking: 13520137
3. Optimistic Concurrency Control: 13521063
4. Multiversion Timestamp Ordering Concurrecy Control: 13521151
5. Eksplorasi Recovery: 13521083