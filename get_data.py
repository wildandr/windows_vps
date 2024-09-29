import MetaTrader5 as mt5
import pandas as pd
import ast  # Untuk parsing string tuple
from datetime import datetime, timedelta
import os  # Untuk pembuatan direktori

# Inisialisasi MetaTrader 5
if not mt5.initialize():
    print("Initialize failed")
    mt5.shutdown()

# Daftar ticker dan timeframe
tickers = ["GOLDm#", "EURUSDm#"]
timeframes = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1
}

# Fungsi untuk membersihkan dan mengekstrak data dari string tuple
def clean_data(row):
    # Parsing string tuple menjadi list menggunakan ast.literal_eval
    try:
        parsed_tuple = ast.literal_eval(row)
        # Pastikan tuple memiliki panjang 8 (sesuai dengan jumlah kolom yang diharapkan)
        if len(parsed_tuple) == 8:
            return parsed_tuple
        else:
            return None  # Kembalikan None jika panjangnya tidak sesuai
    except:
        return None  # Kembalikan None jika parsing gagal

# Loop melalui setiap ticker dan timeframe
for ticker in tickers:
    for tf_name, tf_value in timeframes.items():
        # Variabel untuk menampung data dan waktu sekarang
        all_data = []
        utc_from = datetime.now()  # Mulai dari waktu saat ini
        loop_counter = 0
        last_timestamp = None
        
        print(f"\nFetching data for {ticker} on timeframe {tf_name}")
        
        # Loop untuk mengambil data hingga data tidak ada lagi
        while True:
            loop_counter += 1
            print(f"Fetching bars for {ticker} on timeframe {tf_name} (loop {loop_counter})")
            
            # Ambil data maksimal 10 bar per iterasi
            rates = mt5.copy_rates_from(ticker, tf_value, utc_from, 10)
            
            # Periksa apakah tidak ada data lagi atau data yang diterima kosong
            if rates is None or len(rates) == 0:
                print(f"No more data available for {ticker} on timeframe {tf_name} (loop {loop_counter})")
                break
            
            # Ambil timestamp terakhir dari data yang diambil
            current_last_timestamp = rates[-1]['time']
            
            # Cek apakah data yang diambil sama dengan iterasi sebelumnya
            if current_last_timestamp == last_timestamp:
                print(f"Data has stopped updating for {ticker} on timeframe {tf_name} (loop {loop_counter})")
                break
            
            # Simpan data ke all_data
            all_data.extend(rates)
            last_timestamp = current_last_timestamp
            
            # Set waktu terakhir berdasarkan data terakhir yang diambil untuk loop berikutnya
            utc_from = datetime.utcfromtimestamp(current_last_timestamp) - timedelta(seconds=1)
            print(f"Fetched {len(rates)} bars, total bars so far: {len(all_data)}")
        
        # Jika data tidak kosong, konversi ke DataFrame
        if len(all_data) > 0:
            df = pd.DataFrame(all_data)
            print(f"Columns in fetched data for {ticker} on {tf_name}: {df.columns}")  # Menampilkan kolom yang ada di DataFrame
            
            # Konversi kolom 'time' ke format datetime yang lebih manusiawi
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='s')
                df.rename(columns={'time': 'Time', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'tick_volume': 'Tick Volume', 'spread': 'Spread', 'real_volume': 'Real Volume'}, inplace=True)
            
            # Membuat direktori jika belum ada
            directory = f"datasets/{ticker}"
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            # Simpan DataFrame ke file CSV dengan nama sesuai timeframe
            file_path = f"{directory}/data_{tf_name}.csv"
            df.to_csv(file_path, index=False)
            print(f"Data for {ticker} on {tf_name} successfully saved to {file_path}")
            
            # Bersihkan dan proses data
            df = pd.read_csv(file_path, header=None, names=["raw_data"])
            df["parsed_data"] = df["raw_data"].apply(clean_data)
            df = df.dropna(subset=["parsed_data"])
            df[['timestamp', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']] = pd.DataFrame(df['parsed_data'].tolist(), index=df.index)
            df.drop(columns=["raw_data", "parsed_data"], inplace=True)
            df['time'] = pd.to_datetime(df['timestamp'], unit='s')
            df.drop(columns=['timestamp'], inplace=True)
            df.rename(columns={
                'time': 'Time',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'tick_volume': 'Tick Volume',
                'spread': 'Spread',
                'real_volume': 'Real Volume'
            }, inplace=True)
            
            # Simpan DataFrame yang sudah rapi ke file CSV baru
            output_file = f"{directory}/cleaned_data_{tf_name}.csv"
            df.to_csv(output_file, index=False)
            
            # Cetak hasil data yang sudah rapi
            print(f"Cleaned data for {ticker} on {tf_name}:")
            print(df.head())
            print(f"\nData has been successfully cleaned and saved to {output_file}")
        else:
            print(f"No data found for {ticker} on timeframe {tf_name}.")

# Shutdown MetaTrader 5
mt5.shutdown()
