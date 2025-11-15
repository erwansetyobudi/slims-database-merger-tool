# Database Merger Tool for SLiMS

Tools untuk menggabungkan beberapa database SLiMS menjadi satu database terpadu.

##  Fitur Utama

-  **Merge Multiple Databases** - Gabungkan 2, 3, atau lebih database SLiMS
-  **Auto Increment Handling** - Otomatis menangani konflik ID auto increment
-  **Relational Integrity** - Menjaga hubungan foreign key antar tabel
-  **Flexible Configuration** - Mudah dikonfigurasi untuk berbagai skenario
-  **Data Verification** - Verifikasi hasil merge dengan laporan detail

##  Prerequisites

- Python 3.6+
- MySQL Server
- Akses ke database SLiMS sumber

##  Instalasi

1. **Clone repository ini**
   ```bash
   git clone https://github.com/username/slims-database-merger.git
   cd slims-database-merger
   ```

2. **Install dependencies**
   ```bash
   pip install mysql-connector-python==8.0.32
   ```

3. **Setup konfigurasi**
   ```bash
   python database_merger.py
   ```
   *File `database_config.ini` akan otomatis dibuat*

##  Konfigurasi

Edit file `database_config.ini` dengan informasi database Anda:

###  Contoh untuk 2 Database:
```ini
[TARGET]
host = localhost
database = slims_merged
user = root
password = password_target
port = 3306

[SOURCE_1]
host = localhost
database = slims_perpustakaan_a
user = root
password = password_a
port = 3306

[SOURCE_2]
host = localhost
database = slims_perpustakaan_b
user = root
password = password_b
port = 3306
```

###  Contoh untuk 3 Database:
```ini
[TARGET]
host = localhost
database = slims_merged
user = root
password = password_target
port = 3306

[SOURCE_1]
host = localhost
database = slims_tasik
user = root
password = password1
port = 3306

[SOURCE_2]
host = localhost
database = slims_cirebon
user = root
password = password2
port = 3306

[SOURCE_3]
host = localhost
database = slims_ciamis
user = root
password = password3
port = 3306
```

###  Contoh untuk 4+ Database:
```ini
[TARGET]
host = localhost
database = slims_jabar_merged
user = root
password = password_target
port = 3306

[SOURCE_1]
host = localhost
database = slims_tasik
user = root
password = password1
port = 3306

[SOURCE_2]
host = localhost
database = slims_cirebon
user = root
password = password2
port = 3306

[SOURCE_3]
host = localhost
database = slims_ciamis
user = root
password = password3
port = 3306

[SOURCE_4]
host = localhost
database = slims_garut
user = root
password = password4
port = 3306

# Tambahkan SOURCE_5, SOURCE_6, dst. sesuai kebutuhan
```

##  Cara Penggunaan

1. **Siapkan file konfigurasi** `database_config.ini`

2. **Jalankan tools**:
   ```bash
   python database_merger.py
   ```

3. **Tunggu proses selesai**. Tools akan menampilkan progress:
   -  Analisis struktur database
   -  Pembuatan tabel di database target
   -  Proses merge data
   -  Verifikasi hasil

##  Output yang Dihasilkan

Tools akan menampilkan log detail selama proses:

```
Starting Database Merge Process...
==================================================
Database slims_merged siap
Menganalisis tabel dengan auto increment...
Membuat tabel di database target...
Memulai proses merge data...
Processing database: source_1
Processing database: source_2
Processing database: source_3
Updating auto increment values...
Verifying merge results...
Merge process completed!
```

##  Struktur File

```
slims-database-merger/
├── database_merger.py          # Main script
├── database_config.ini         # File konfigurasi (auto-generated)
└── README.md                   # Dokumentasi ini
```

##  Troubleshooting

###  Error Koneksi Database
- Pastikan MySQL server running
- Periksa username/password di `database_config.ini`
- Pastikan user memiliki hak akses yang cukup

###  Error During Merge
- Pastikan semua database sumber memiliki struktur tabel yang sama
- Cek kapasitas storage database target
- Pastikan tidak ada koneksi lain ke database selama proses merge

###  Data Duplicate
- Tools menggunakan `INSERT IGNORE` untuk menghindari duplikasi
- Data dengan primary key sama akan di-skip

##  Catatan Penting

1. **Backup Database**: Selalu backup database sebelum melakukan merge
2. **Waktu Proses**: Proses bisa memakan waktu lama untuk database besar
3. **Storage**: Pastikan storage cukup untuk database hasil merge
4. **Testing**: Test di environment development terlebih dahulu

##  Development

### Menambah Fitur
Script dirancang modular sehingga mudah dikembangkan:

```python
# Menambah custom logic
def custom_data_processing(self, table_name, data):
    # Tambahkan logic custom di sini
    pass
```

### Debug Mode
Untuk detail log yang lebih lengkap, tambahkan print statement di method yang diinginkan.

##  License

MIT License - bebas digunakan untuk keperluan personal maupun komersial.

##  Contributing

Pull requests dipersilakan! Untuk perubahan besar, buka issue terlebih dahulu.

##  Support

Jika mengalami masalah:
1. Cek section Troubleshooting di atas
2. Buat issue di GitHub repository
3. Lampirkan file log dan konfigurasi (tanpa password)

---

**Happy Merging!** 
