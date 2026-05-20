from flask import Flask, jsonify, request
from dateutil import parser
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

app = Flask(__name__)

INFLUXDB_URL="http://influxdb:8086"
INFLUXDB_TOKEN="1jxpyy51EgsZiH7grClMk5ta9TeeOyIs3B7UscBWWhVWgH8JFZPTdY57FIdEE5MGm20-pbTpt419vRRZAWVuqg=="
INFLUXDB_ORG="knowledgehub"
INFLUXDB_BUCKET="metrics"

latest_metrics_storage = []

"""Health check endpoint to verify the API is running and responsive."""
@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200
    
    
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

"""Endpoint to receive metrics data from the collector script."""
@app.route('/metrics', methods=['POST'])
def receive_metrics():
    if not request.is_json:
        return jsonify({'error': 'Must be JSON'}), 400
    
    data = request.json
    
    # 1. NORMALISASI: Pastikan data selalu berbentuk list agar aman saat di-looping
    if isinstance(data, list):
        metrics_list = data
    else:
        metrics_list = [data] # Bungkus menjadi list jika hanya 1 objek
    
    # 2. SIMPAN KE MEMORY (Sesuai kode lama Anda untuk dilihat di browser)
    latest_metrics_storage.extend(metrics_list)
    if len(latest_metrics_storage) > 100:
        del latest_metrics_storage[:-100] # Hapus data lama, simpan 100 terbaru saja

    # 3. KIRIM KE INFLUXDB (Proses konversi)
    try:
        # Looping setiap item di dalam array
        for item in metrics_list:
            container_name = item.get("container_name", "unknown")
            timestamp_str = item.get("timestamp")
            metrics = item.get("metrics", {})
            
            cpu_val = metrics.get("cpu", 0)
            mem_val = metrics.get("memory", 0)
            disk_val = metrics.get("disk", 0)

            # Fix format waktu RFC3339 ke format yang dipahami Python
            
            if timestamp_str:
                try:
                    # 1. Tangani jika ada akhiran 'Z'
                    if timestamp_str.endswith('Z'):
                        timestamp_str = timestamp_str[:-1] + '+00:00'
                    
                    # 2. Potong paksa nanodetik (9 digit) menjadi mikrodetik (6 digit)
                    if '.' in timestamp_str and '+' in timestamp_str:
                        bagian_depan, bagian_belakang = timestamp_str.split('.')
                        digit_mikro, timezone = bagian_belakang.split('+')
                        # Potong string mikrodetik hanya ambil 6 angka pertama
                        timestamp_str = f"{bagian_depan}.{digit_mikro[:6]}+{timezone}"
                    
                    # 3. Parsing menggunakan datetime bawaan (aman karena sudah dipotong)
                    dt = datetime.fromisoformat(timestamp_str)
                except Exception as e:
                    print(f"Error when parsing timestamp: {e}")
                    dt = datetime.utcnow()
            else:
                dt = datetime.utcnow()

            # Buat Point (Format Line Protocol InfluxDB)
            point = Point("container_metrics") \
                .tag("container_name", container_name) \
                .field("cpu_usage", float(cpu_val)) \
                .field("memory_usage", float(mem_val)) \
                .field("disk_usage", float(disk_val)) \
                .time(dt)

            # Eksekusi Write ke InfluxDB
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        print(f"Data received and saved to InfluxDB at {datetime.now()}")
        return jsonify({'message': f'Successfully processed {len(metrics_list)} metrics'}), 201

    except Exception as e:
        print(f"Error InfluxDB: {str(e)}")
        return jsonify({'error': 'Internal Server Error', 'details': str(e)}), 500

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Endpoint to retrieve metrics data."""
    return jsonify(latest_metrics_storage), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)