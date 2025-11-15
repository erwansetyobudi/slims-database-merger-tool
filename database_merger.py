import mysql.connector
from mysql.connector import Error
import configparser
import os
from datetime import datetime

class DatabaseMerger:
    def __init__(self, config_file='database_config.ini'):
        self.config_file = config_file
        self.databases = {}
        self.target_db = None
        self.auto_increment_tables = {}
        self.relations = {}
        
    def load_config(self):
        """Load konfigurasi database dari file"""
        config = configparser.ConfigParser()
        config.read(self.config_file)
        
        # Konfigurasi database target
        self.target_db = {
            'host': config['TARGET']['host'],
            'database': config['TARGET']['database'],
            'user': config['TARGET']['user'],
            'password': config['TARGET']['password'],
            'port': config.getint('TARGET', 'port', fallback=3306)
        }
        
        # Konfigurasi database sumber
        for i in range(1, 4):
            section = f'SOURCE_{i}'
            if section in config:
                self.databases[f'source_{i}'] = {
                    'host': config[section]['host'],
                    'database': config[section]['database'],
                    'user': config[section]['user'],
                    'password': config[section]['password'],
                    'port': config.getint(section, 'port', fallback=3306)
                }
    
    def create_target_database(self):
        """Membuat database target jika belum ada"""
        try:
            # Koneksi tanpa database spesifik
            conn_config = self.target_db.copy()
            conn_config.pop('database', None)
            
            conn = mysql.connector.connect(**conn_config)
            cursor = conn.cursor()
            
            # Create database jika belum ada
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.target_db['database']}")
            print(f"Database {self.target_db['database']} siap")
            
            cursor.close()
            conn.close()
            
        except Error as e:
            print(f"Error creating database: {e}")
    
    def get_connection(self, db_config):
        """Membuat koneksi ke database"""
        try:
            conn = mysql.connector.connect(**db_config)
            return conn
        except Error as e:
            print(f"Error connecting to database {db_config.get('database', 'unknown')}: {e}")
            return None
    
    def analyze_auto_increment_tables(self):
        """Menganalisis tabel dengan auto increment"""
        print("Menganalisis tabel dengan auto increment...")
        
        for db_name, db_config in self.databases.items():
            conn = self.get_connection(db_config)
            if conn:
                cursor = conn.cursor()
                
                # Query untuk mendapatkan tabel dengan auto increment
                query = """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s 
                AND EXTRA LIKE '%auto_increment%'
                """
                
                cursor.execute(query, (db_config['database'],))
                results = cursor.fetchall()
                
                for table_name, column_name, data_type, extra in results:
                    if table_name not in self.auto_increment_tables:
                        self.auto_increment_tables[table_name] = {
                            'column': column_name,
                            'data_type': data_type,
                            'max_values': {}
                        }
                    
                    # Dapatkan nilai maksimum saat ini
                    try:
                        max_query = f"SELECT MAX({column_name}) FROM {table_name}"
                        cursor.execute(max_query)
                        max_val = cursor.fetchone()[0] or 0
                        self.auto_increment_tables[table_name]['max_values'][db_name] = max_val
                    except Error as e:
                        print(f"    Error getting max value for {table_name}.{column_name}: {e}")
                        self.auto_increment_tables[table_name]['max_values'][db_name] = 0
                
                cursor.close()
                conn.close()
        
        # Print hasil analisis
        for table, info in self.auto_increment_tables.items():
            print(f"Tabel: {table}, Kolom: {info['column']}")
            for db, max_val in info['max_values'].items():
                print(f"  {db}: max_value = {max_val}")
    
    def analyze_relations(self):
        """Menganalisis hubungan antar tabel"""
        print("Menganalisis hubungan antar tabel...")
        
        # Definisikan hubungan berdasarkan struktur database
        self.relations = {
            'biblio': {
                'primary_key': 'biblio_id',
                'related_tables': [
                    'biblio_attachment', 'biblio_author', 'biblio_custom', 
                    'biblio_log', 'biblio_relation', 'biblio_topic',
                    'comment', 'item', 'reserve', 'search_biblio', 'serial'
                ]
            },
            'item': {
                'primary_key': 'item_id',
                'related_tables': ['loan', 'stock_take_item']
            },
            'member': {
                'primary_key': 'member_id',
                'related_tables': ['comment', 'fines', 'loan', 'reserve', 'visitor_count']
            },
            'mst_author': {
                'primary_key': 'author_id',
                'related_tables': ['biblio_author']
            },
            'mst_topic': {
                'primary_key': 'topic_id',
                'related_tables': ['biblio_topic']
            },
            'files': {
                'primary_key': 'file_id',
                'related_tables': ['biblio_attachment', 'files_read']
            },
            'user': {
                'primary_key': 'user_id',
                'related_tables': ['backup_log', 'biblio_log', 'system_log']
            },
            'mst_gmd': {
                'primary_key': 'gmd_id',
                'related_tables': ['biblio', 'serial']
            },
            'mst_publisher': {
                'primary_key': 'publisher_id',
                'related_tables': ['biblio']
            },
            'mst_language': {
                'primary_key': 'language_id',
                'related_tables': ['biblio']
            },
            'mst_place': {
                'primary_key': 'place_id',
                'related_tables': ['biblio']
            }
        }
    
    def create_tables_in_target(self):
        """Membuat tabel di database target berdasarkan struktur dari source pertama"""
        print("Membuat tabel di database target...")
        
        source_config = list(self.databases.values())[0]
        conn_source = self.get_connection(source_config)
        conn_target = self.get_connection(self.target_db)
        
        if conn_source and conn_target:
            cursor_source = conn_source.cursor()
            cursor_target = conn_target.cursor()
            
            try:
                # Dapatkan daftar tabel
                cursor_source.execute("SHOW TABLES")
                tables = [table[0] for table in cursor_source.fetchall()]
                
                for table_name in tables:
                    # Dapatkan CREATE TABLE statement
                    cursor_source.execute(f"SHOW CREATE TABLE {table_name}")
                    create_result = cursor_source.fetchone()
                    if create_result:
                        create_stmt = create_result[1]
                        
                        # Eksekusi di target
                        cursor_target.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cursor_target.execute(create_stmt)
                        print(f"Tabel {table_name} dibuat")
                
                conn_target.commit()
                
            except Error as e:
                print(f"Error creating tables: {e}")
                conn_target.rollback()
            finally:
                cursor_source.close()
                cursor_target.close()
                conn_source.close()
                conn_target.close()
    
    def merge_data(self):
        """Proses merge data dari semua database source ke target"""
        print("Memulai proses merge data...")
        
        # Offset untuk auto increment values
        offsets = {}
        current_max_values = {}
        
        # Hitung offset untuk setiap tabel auto increment
        for table_name, info in self.auto_increment_tables.items():
            current_max = 0
            offsets[table_name] = {}
            
            for db_name in self.databases.keys():
                offsets[table_name][db_name] = current_max
                current_max += info['max_values'].get(db_name, 0)
            
            current_max_values[table_name] = current_max
        
        # Proses merge untuk setiap database source
        for db_name, db_config in self.databases.items():
            print(f"\nProcessing database: {db_name}")
            self.merge_database_data(db_name, db_config, offsets)
        
        # Update auto increment values di target
        self.update_auto_increment_values(current_max_values)
    
    def merge_database_data(self, db_name, db_config, offsets):
        """Merge data dari satu database source"""
        conn_source = self.get_connection(db_config)
        conn_target = self.get_connection(self.target_db)
        
        if not conn_source or not conn_target:
            print(f"  Tidak dapat terkoneksi ke database {db_name}")
            return
        
        cursor_source = conn_source.cursor()
        cursor_target = conn_target.cursor()
        
        try:
            # Dapatkan daftar tabel
            cursor_source.execute("SHOW TABLES")
            tables = [table[0] for table in cursor_source.fetchall()]
            
            # Urutan proses berdasarkan dependencies
            processing_order = self.get_processing_order(tables)
            
            for table_name in processing_order:
                if table_name not in tables:
                    continue
                    
                print(f"  Memproses tabel: {table_name}")
                
                # Dapatkan semua data dari source
                cursor_source.execute(f"SELECT * FROM {table_name}")
                rows = cursor_source.fetchall()
                
                if not rows:
                    print(f"    Tabel {table_name} kosong, dilewati")
                    continue
                
                # Dapatkan nama kolom
                cursor_source.execute(f"DESCRIBE {table_name}")
                columns_info = cursor_source.fetchall()
                columns = [col[0] for col in columns_info]
                
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                insert_count = 0
                skip_count = 0
                
                for row in rows:
                    values = list(row)
                    
                    # Handle auto increment columns
                    if table_name in self.auto_increment_tables:
                        ai_column = self.auto_increment_tables[table_name]['column']
                        if ai_column in columns:
                            col_index = columns.index(ai_column)
                            if values[col_index] is not None:
                                # Apply offset
                                original_value = values[col_index]
                                new_value = original_value + offsets[table_name].get(db_name, 0)
                                values[col_index] = new_value
                    
                    # Handle foreign keys
                    values = self.update_foreign_keys(table_name, values, columns, offsets, db_name)
                    
                    # Insert data
                    try:
                        insert_query = f"INSERT IGNORE INTO {table_name} ({column_names}) VALUES ({placeholders})"
                        cursor_target.execute(insert_query, values)
                        if cursor_target.rowcount > 0:
                            insert_count += 1
                        else:
                            skip_count += 1
                    except Error as e:
                        print(f"    Error inserting into {table_name}: {e}")
                        # Coba insert dengan approach berbeda untuk error tertentu
                        try:
                            # Untuk error duplicate, coba dengan approach berbeda
                            insert_query = f"REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
                            cursor_target.execute(insert_query, values)
                            if cursor_target.rowcount > 0:
                                insert_count += 1
                        except Error as e2:
                            print(f"    Juga gagal dengan REPLACE: {e2}")
                            skip_count += 1
                
                conn_target.commit()
                print(f"    {insert_count} records inserted, {skip_count} skipped in {table_name}")
        
        except Error as e:
            print(f"Error processing {db_name}: {e}")
            conn_target.rollback()
        finally:
            cursor_source.close()
            cursor_target.close()
            conn_source.close()
            conn_target.close()
    
    def get_processing_order(self, tables):
        """Mendapatkan urutan proses berdasarkan dependencies"""
        # Prioritaskan tabel master/referensi terlebih dahulu
        master_tables = [
            'mst_gmd', 'mst_author', 'mst_topic', 'mst_publisher', 'mst_language',
            'mst_place', 'mst_coll_type', 'mst_location', 'mst_item_status',
            'mst_member_type', 'user_group', 'mst_module', 'mst_carrier_type',
            'mst_content_type', 'mst_media_type', 'mst_frequency', 'mst_label',
            'mst_loan_rules', 'mst_relation_term', 'mst_servers', 'mst_supplier'
        ]
        
        # Kemudian tabel utama
        main_tables = [
            'user', 'member', 'biblio', 'item', 'files', 'content'
        ]
        
        # Terakhir tabel transaksi dan relasi
        transaction_tables = [
            table for table in tables 
            if table not in master_tables and table not in main_tables
        ]
        
        # Filter hanya tabel yang ada
        ordered_tables = []
        for table in master_tables + main_tables + transaction_tables:
            if table in tables:
                ordered_tables.append(table)
        
        return ordered_tables
    
    def update_foreign_keys(self, table_name, values, columns, offsets, db_name):
        """Update nilai foreign keys berdasarkan offset"""
        updated_values = values.copy()
        
        for related_table, relation_info in self.relations.items():
            if table_name in relation_info['related_tables']:
                fk_column = relation_info['primary_key']
                if fk_column in columns:
                    idx = columns.index(fk_column)
                    if updated_values[idx] is not None:
                        # Apply offset jika tabel referensi memiliki auto increment
                        if related_table in offsets:
                            updated_values[idx] += offsets[related_table].get(db_name, 0)
        
        return updated_values
    
    def update_auto_increment_values(self, current_max_values):
        """Update nilai auto increment di tabel target"""
        print("\nUpdating auto increment values...")
        
        conn = self.get_connection(self.target_db)
        if not conn:
            return
        
        cursor = conn.cursor()
        
        try:
            for table_name, max_value in current_max_values.items():
                if max_value > 0:
                    try:
                        # Set AUTO_INCREMENT ke nilai berikutnya
                        set_query = f"ALTER TABLE {table_name} AUTO_INCREMENT = {max_value + 1}"
                        cursor.execute(set_query)
                        print(f"  Set {table_name}.AUTO_INCREMENT = {max_value + 1}")
                    except Error as e:
                        print(f"  Error setting auto increment for {table_name}: {e}")
            
            conn.commit()
        except Error as e:
            print(f"Error updating auto increment: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def verify_merge(self):
        """Verifikasi hasil merge"""
        print("\nVerifying merge results...")
        
        conn = self.get_connection(self.target_db)
        if not conn:
            return
        
        cursor = conn.cursor()
        
        try:
            # Hitung total records dari semua source databases
            total_expected = {}
            for db_name, db_config in self.databases.items():
                conn_source = self.get_connection(db_config)
                if conn_source:
                    cursor_source = conn_source.cursor()
                    cursor_source.execute("SHOW TABLES")
                    tables = [table[0] for table in cursor_source.fetchall()]
                    
                    for table in tables:
                        try:
                            cursor_source.execute(f"SELECT COUNT(*) as count FROM {table}")
                            count = cursor_source.fetchone()[0]
                            
                            if table not in total_expected:
                                total_expected[table] = 0
                            total_expected[table] += count
                        except Error as e:
                            print(f"    Error counting {table} in {db_name}: {e}")
                    
                    cursor_source.close()
                    conn_source.close()
            
            # Bandingkan dengan target
            print("\nMerge Verification Results:")
            print("Table Name".ljust(30) + "Expected".ljust(12) + "Actual".ljust(12) + "Status")
            print("-" * 60)
            
            cursor.execute("SHOW TABLES")
            target_tables = [table[0] for table in cursor.fetchall()]
            
            for table in target_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                    actual = cursor.fetchone()[0]
                    expected = total_expected.get(table, 0)
                    
                    status = "OK" if actual >= expected else "MISSING"
                    print(f"{table.ljust(30)}{str(expected).ljust(12)}{str(actual).ljust(12)}{status}")
                except Error as e:
                    print(f"    Error counting {table} in target: {e}")
        
        except Error as e:
            print(f"Error during verification: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def run_merge(self):
        """Jalankan proses merge lengkap"""
        print("Starting Database Merge Process...")
        print("=" * 50)
        
        # Load configuration
        if not os.path.exists(self.config_file):
            print(f"File konfigurasi {self.config_file} tidak ditemukan!")
            return
        
        self.load_config()
        
        # Create target database
        self.create_target_database()
        
        # Analyze database structure
        self.analyze_auto_increment_tables()
        self.analyze_relations()
        
        # Create tables in target
        self.create_tables_in_target()
        
        # Merge data
        self.merge_data()
        
        # Verify results
        self.verify_merge()
        
        print("\nMerge process completed!")

def create_config_file():
    """Membuat file konfigurasi contoh"""
    config_content = """[TARGET]
host = localhost
database = slims_merged
user = root
password = root
port = 3306

[SOURCE_1]
host = localhost
database = slims_a
user = root
password = root
port = 3306

[SOURCE_2]
host = localhost
database = slims_b
user = root
password = root
port = 3306

[SOURCE_3]
host = localhost
database = slims_c
user = root
password = root
port = 3306
"""
    
    with open('database_config.ini', 'w') as f:
        f.write(config_content)
    
    print("File konfigurasi 'database_config.ini' telah dibuat.")
    print("Silakan edit file tersebut dengan informasi database yang sesuai.")

if __name__ == "__main__":
    # Buat file konfigurasi jika belum ada
    if not os.path.exists('database_config.ini'):
        create_config_file()
        print("\nSilakan edit database_config.ini dengan kredensial database Anda, lalu jalankan script lagi.")
    else:
        # Jalankan merge process
        merger = DatabaseMerger()
        merger.run_merge()
