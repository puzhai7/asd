import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import clickhouse_connect

# Настройки из Docker Compose
CH_HOST = os.getenv('CH_HOST', 'ch-s1-r1')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASS = os.getenv('CH_PASSWORD', '')
PORT = int(os.getenv('EXPORTER_PORT', '8080'))

def get_clickhouse_metrics():
    try:
        client = clickhouse_connect.get_client(host=CH_HOST, user=CH_USER, password=CH_PASS)
        
        # ЗАПРОС 1: Нагрузка по ЧТЕНИЮ 
        read_query = """
        SELECT user, arrayJoin(tables) as tbl, sum(read_bytes), count()
        FROM system.query_log 
        WHERE event_date >= today() AND event_time > now() - interval 1 minute 
          AND type = 'QueryFinish' AND query_kind = 'Select' AND tbl != ''
        GROUP BY user, tbl
        """
        
        # ЗАПРОС 2: Нагрузка по ЗАПИСИ 
        write_query = """
        SELECT user, arrayJoin(tables) as tbl, sum(written_rows), sum(written_bytes), count()
        FROM system.query_log 
        WHERE event_date >= today() AND event_time > now() - interval 1 minute 
          AND type = 'QueryFinish' AND query_kind = 'Insert' AND tbl != ''
        GROUP BY user, tbl
        """
        
        metrics = []
        
        # Обработка чтения
        for row in client.query(read_query).result_rows:
            user, table, b, q = row
            metrics.append(f'ch_usage_read_bytes{{user="{user}",table="{table}"}} {b}')
            metrics.append(f'ch_usage_read_queries{{user="{user}",table="{table}"}} {q}')

        # Обработка записи
        for row in client.query(write_query).result_rows:
            user, table, rows, bytes_wr, q = row
            metrics.append(f'ch_usage_write_rows{{user="{user}",table="{table}"}} {rows}')
            metrics.append(f'ch_usage_write_bytes{{user="{user}",table="{table}"}} {bytes_wr}')
            metrics.append(f'ch_usage_write_queries{{user="{user}",table="{table}"}} {q}')

        return "\n".join(metrics)
    except Exception as e:
        return f"# Error: {str(e)}"
        
if __name__ == '__main__':
    print(f"Starting Python Exporter on port {PORT}...")
    server = HTTPServer(('0.0.0.0', PORT), MetricsHandler)
    server.serve_forever()
