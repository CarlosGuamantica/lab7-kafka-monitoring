from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

# Define tu topic
topic = 'movielog1'

# Iniciar servidor de métricas en el puerto 8765
start_http_server(8765)

# Definir métricas
REQUEST_COUNT = Counter(
    'request_count_total', 'Recommendation Request Count',
    ['http_status']
)

REQUEST_LATENCY = Histogram(
    'request_latency_seconds', 'Request latency'
)

def main():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id=topic,
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    for message in consumer:
        event = message.value.decode('utf-8')
        values = event.split(',')
        if 'recommendation request' in values[2]:
            status_code = values[3].strip()
            REQUEST_COUNT.labels(http_status=status_code).inc()
            try:
                time_taken = float(values[-1].strip().split(" ")[0])
                REQUEST_LATENCY.observe(time_taken / 1000)
            except:
                print(f"Error procesando tiempo: {values[-1]}")

if __name__ == "__main__":
    main()
