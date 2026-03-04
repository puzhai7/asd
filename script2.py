import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import clickhouse_connect
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Конфигурация из окружения
CH_HOST = os.getenv('CH_HOST', 'ch-s1-r1')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASS = os.getenv('CH_PASSWORD', 'SRE_secure_2026')
PORT = int(os.getenv('EXPORTER_PORT', '8080'))

def get_metrics():
    metrics = []
    client = None
    try:
        # Подключение к CH
        client = clickhouse_connect.get_client(
            host=CH_HOST, 
            user=CH_USER, 
            password=CH_PASS,
            connect_timeout=5
        )

        # 1. Метрики ЧТЕНИЯ (за последнюю минуту)
        read_res = client.query("""
            SELECT user, arrayJoin(tables) as tbl, sum(read_bytes) as b, count() as q
            FROM system.query_log 
            WHERE event_date >= today() AND event_time > now() - interval 1 minute 
              AND type = 'QueryFinish' AND query_kind = 'Select' AND tbl != ''
            GROUP BY user, tbl
        """)
        for row in read_res.result_rows:
            metrics.append(f'ch_usage_read_bytes{{user="{row[0]}",table="{row[1]}"}} {row[2]}')
            metrics.append(f'ch_usage_read_queries{{user="{row[0]}",table="{row[1]}"}} {row[3]}')

        # 2. Метрики ЗАПИСИ (за последнюю минуту)
        write_res = client.query("""
            SELECT user, arrayJoin(tables) as tbl, sum(written_rows) as r, sum(written_bytes) as b, count() as q
            FROM system.query_log 
            WHERE event_date >= today() AND event_time > now() - interval 1 minute 
              AND type = 'QueryFinish' AND query_kind = 'Insert' AND tbl != ''
            GROUP BY user, tbl
        """)
        for row in write_res.result_rows:
            metrics.append(f'ch_usage_write_rows{{user="{row[0]}",table="{row[1]}"}} {row[2]}')
            metrics.append(f'ch_usage_write_bytes{{user="{row[0]}",table="{row[1]}"}} {row[3]}')
            metrics.append(f'ch_usage_write_queries{{user="{row[0]}",table="{row[1]}"}} {row[4]}')

        # 3. Метрики фоновых процессов (Merges)
        merge_res = client.query("SELECT table, count() FROM system.merges GROUP BY table")
        for row in merge_res.result_rows:
            metrics.append(f'ch_active_merges{{table="{row[0]}"}} {row[1]}')

    except Exception as e:
        logging.error(f"Error fetching metrics: {e}")
        return f"# ERROR: {str(e)}"
    finally:
        if client:
            client.close()

    return "\n".join(metrics)

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            
            output = "# HELP ch_usage_read_bytes Total bytes read\n"
            output += "# TYPE ch_usage_read_bytes gauge\n"
            output += get_metrics()
            
            self.wfile.write(output.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return # Отключаем лишний лог запросов в консоль

if __name__ == '__main__':
    logging.info(f"Starting SRE Exporter on port {PORT} targeting {CH_HOST}...")
    server = HTTPServer(('0.0.0.0', PORT), MetricsHandler)
    server.serve_forever()
