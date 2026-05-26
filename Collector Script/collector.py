import time
import requests
import os
import json

# --- CONFIGURATION ---
CADVISOR_URL = "http://cadvisor:8080/api/v1.3/docker/"
API_URL = "https://aca-monitoring-api.politesea-d4b7dfa0.spaincentral.azurecontainerapps.io/metrics"
POLL_INTERVAL = 10 
QUEUE_FILE = "metrics_queue.json"

def fetch_cadvisor_data():
    """Mengambil data mentah dari API cAdvisor."""
    try:
        response = requests.get(CADVISOR_URL, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching cAdvisor: {e}")
        return None

def process_metrics(raw_data):
    """Parsing raw data to JSON format."""
    processed_list = []
    
    """"Filter only entriest that are containers"""
    for container_id, info in raw_data.items():
        """Skip if entry is not a dict"""
        if not isinstance(info, dict):
            continue
            
        """Skip if stats is missing or empty"""
        if 'stats' not in info or not info['stats']:
            continue
            
        try:
            latest_stats = info['stats'][-1]
            
            payload = {
                "container_name": info.get('aliases', [container_id])[0] if isinstance(info.get('aliases'), list) else container_id,
                "timestamp": latest_stats.get('timestamp', time.time()),
                "metrics": {
                    "cpu": latest_stats.get('cpu', {}).get('usage', {}).get('total', 0),
                    "memory": latest_stats.get('memory', {}).get('usage', 0),
                    "disk": latest_stats.get('filesystem', [{}])[0].get('usage', 0) if latest_stats.get('filesystem') else 0
                }
            }
            processed_list.append(payload)
            
        except (KeyError, IndexError, TypeError) as e:
            print(f"Warning: Failed to parse {container_id}: {e}")
            continue
    
    return processed_list

def save_to_queue(data):
    """Saving data to local queue file if API is unreachable."""
    queue = []
    if os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE, 'r') as f:
            try:
                queue = json.load(f)
            except: queue = []
            
    queue.extend(data)
    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=4)
    print(f"Data saved to queue ({len(queue)} items total).")

def send_to_api(data):
    """Send data to API, if fails save to queue."""
    if os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE, 'r') as f:
            old_queue = json.load(f)
        
        if old_queue:
            print("Trying to send old queue data...")
            try:
                requests.post(API_URL, json=old_queue, timeout=5)
                os.remove(QUEUE_FILE)
                print("Old queue data sent successfully.")
            except:
                print("API still unreachable, adding new data to queue.")
                save_to_queue(data)
                return
            
    try:
        response = requests.post(API_URL, json=data, timeout=5)
        response.raise_for_status()
        print("Data sent to API successfully.")
    except Exception as e:
        print(f"Failed to send data to API: {e}")
        save_to_queue(data)

def main():
    print("Monitoring Collector Started...")
    while True:
        raw = fetch_cadvisor_data()
        if raw:
            metrics = process_metrics(raw)
            if metrics:
                send_to_api(metrics)
        
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()