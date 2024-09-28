from flask import Flask, request, jsonify
import json
from send_to_telegram import send_to_telegram  # pastikan modul ini sudah tersedia atau buat fungsi send_to_telegram

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # Mengambil data JSON dari TradingView
        data = request.json
        if data:
            # Konversi data ke string JSON
            json_data = json.dumps(data)
            
            # Mengirim pesan ke Telegram menggunakan fungsi yang sudah ada
            send_to_telegram(json_data)
            
            return jsonify({"status": "success", "message": "Webhook received and sent to Telegram!"}), 200
        else:
            return jsonify({"status": "fail", "message": "No data received"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
