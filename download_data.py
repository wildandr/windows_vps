import MetaTrader5 as mt5
import pandas as pd
import h5py
import os
from datetime import datetime, timedelta

# Inisialisasi MetaTrader 5
mt5.initialize()

# Simbol yang ingin diambil
symbols = ['GOLDm#', 'EURAUDm#']

# Timeframes yang ingin diambil dan jumlah bar yang akan diambil per batch
timeframes = {
    "m1": (mt5.TIMEFRAME_M1, 1000),
    "m5": (mt5.TIMEFRAME_M5, 1000),
    "m15": (mt5.TIMEFRAME_M15, 1000),
    "m30": (mt5.TIMEFRAME_M30, 1000),
    "h1": (mt5.TIMEFRAME_H1, 1000),
    "h4": (mt5.TIMEFRAME_H4, 1000),
    "h6": (mt5.TIMEFRAME_H6, 1000)
}

# Fungsi untuk mendapatkan batas tanggal terakhir hingga market tutup Jumat (UTC-4)
def get_last_friday_end_time():
    now = datetime.now() - timedelta(hours=4)  # Konversi ke UTC-4
    last_friday = now - timedelta(days=(now.weekday() + 2) % 7 + 2)
    close_time = last_friday.replace(hour=16, minute=0, second=0, microsecond=0)
    return close_time

# Tentukan batas waktu hingga penutupan Jumat kemarin dalam UTC-4
end_time = get_last_friday_end_time()

# Folder penyimpanan
base_folder = "datasets"

# Ambil data untuk setiap simbol dan timeframe
for symbol in symbols:
    print(f"Fetching data for {symbol}")
    symbol_folder = os.path.join(base_folder, symbol)
    
    # Buat folder untuk simbol jika belum ada
    if not os.path.exists(symbol_folder):
        os.makedirs(symbol_folder)

    for tf_name, (tf, num_bars) in timeframes.items():
        print(f"Fetching timeframe {tf_name} for {symbol}")
        timeframe_folder = os.path.join(symbol_folder, tf_name)

        # Buat folder untuk timeframe jika belum ada
        if not os.path.exists(timeframe_folder):
            os.makedirs(timeframe_folder)

        current_time = end_time
        all_data = []
        final_date = None

        # Iterasi untuk mengambil data dalam batch sesuai num_bars
        while True:
            # Ambil data dari MT5
            rates = mt5.copy_rates_from(symbol, tf, current_time, num_bars)
            
            # Jika tidak ada data yang diambil, berhenti
            if rates is None or len(rates) == 0:
                print(f"No more data for {symbol} in timeframe {tf_name} up to {current_time}")
                break
            
            # Jika hanya satu bar diambil berulang kali atau kurang dari num_bars, berhenti
            if len(rates) < num_bars:
                print(f"Reached the end of available data for {symbol} in timeframe {tf_name}")
                break

            # Tambahkan data ke list all_data
            all_data.extend(rates)

            # Cek apakah current_time berubah
            new_time = datetime.utcfromtimestamp(rates[0]['time'])
            if new_time == current_time:
                print(f"Stuck at {new_time} for {symbol} in {tf_name}, stopping to avoid infinite loop.")
                break
            
            # Update current_time ke bar paling awal dari batch
            current_time = new_time

            print(f"Fetched {len(rates)} bars from {current_time} for {tf_name}")

        # Cetak batas waktu terakhir yang diambil dalam UTC-4
        if all_data:
            final_date = datetime.utcfromtimestamp(all_data[-1]['time']) - timedelta(hours=4)
            print(f"Timeframe {tf_name} for {symbol} reached back to {final_date.strftime('%Y-%m-%d %H:%M:%S')} (UTC-4)")

        # Jika data ditemukan, simpan dalam HDF5
        if all_data:
            df = pd.DataFrame(all_data)

            # Cek kolom yang ada dalam DataFrame
            print(f"Kolom yang tersedia: {df.columns}")

            # Konversi kolom 'time' ke datetime dan simpan ke format HDF5
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='s')
            else:
                print(f"'time' column not found, columns available: {df.columns}")
                continue  # Jika 'time' tidak ada, lewati proses penyimpanan untuk dataset ini

            # Simpan data per hari dalam format HDF5
            grouped = df.groupby(df['time'].dt.date)
            for date, group in grouped:
                filename = group['time'].dt.strftime('%H%M%d%m').iloc[0] + ".h5"
                hdf5_path = os.path.join(timeframe_folder, filename)
                with h5py.File(hdf5_path, 'w') as hdf:
                    df_hdf = group.to_numpy()
                    hdf.create_dataset(tf_name, data=df_hdf)
                    print(f"Saved data for {symbol} - {tf_name} to {hdf5_path}")

# Shutdown MetaTrader 5
mt5.shutdown()
