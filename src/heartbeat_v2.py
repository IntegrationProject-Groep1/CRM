import pika
import time
import signal
import sys
import uuid
from datetime import datetime
from sf_connection import SFConnection


class Heartbeat:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = "heartbeat"
        self.interval = 1  # seconds
        self.health_check_counter = 0
        self.sf_healthy = True
        self.uptime = 0
        self.start_time = time.time()
        self.shutting_down = False
        self.sf = SFConnection()

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters("localhost")
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue, durable=True)
            print(
                f"[Heartbeat] Connected to RabbitMQ, queue: {self.queue}"
            )
            return True
        except Exception as error:
            print(
                f"[Heartbeat] Failed to connect to RabbitMQ: {str(error)}"
            )
            return False

    def check_salesforce_health(self):
        try:
            is_healthy = self.sf.health_check()
            self.sf_healthy = is_healthy
            if is_healthy:
                print("[Heartbeat] Salesforce health check: OK")
            else:
                print("[Heartbeat] Salesforce health check: FAILED")
            return is_healthy
        except Exception as error:
            print(
                f"[Heartbeat] Salesforce health check error: {str(error)}"
            )
            self.sf_healthy = False
            return False

    def get_status(self):
        if self.shutting_down:
            return "offline"
        return "online" if self.sf_healthy else "degraded"

    def generate_heartbeat(self):
        now = datetime.utcnow().isoformat() + "Z"
        message_id = f"hb-crm-{str(uuid.uuid4())}"
        status = self.get_status()
        self.uptime = int(time.time() - self.start_time)

        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>{message_id}</message_id>
    <version>2.0</version>
    <type>heartbeat</type>
    <timestamp>{now}</timestamp>
    <source>crm</source>
  </header>
  <body>
    <status>{status}</status>
    <service>crm</service>
    <uptime>{self.uptime}</uptime>
  </body>
</message>"""

        return xml

    def send_heartbeat(self):
        try:
            self.health_check_counter += 1

            if self.health_check_counter % 10 == 0:
                self.check_salesforce_health()

            heartbeat = self.generate_heartbeat()

            if self.channel:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=self.queue,
                    body=heartbeat,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print(
                    f"[Heartbeat] Sent ({self.get_status()}) - Counter: {self.health_check_counter}"
                )
        except Exception as error:
            print(f"[Heartbeat] Failed to send heartbeat: {str(error)}")
            self.reconnect()

    def reconnect(self):
        print("[Heartbeat] Attempting to reconnect to RabbitMQ...")
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass

        if not self.connect():
            time.sleep(5)
            self.reconnect()

    def start(self):
        print("[Heartbeat] Starting heartbeat service...")
        self.connect()
        print("[Heartbeat] Heartbeat interval set to 1 second")

        try:
            while True:
                self.send_heartbeat()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        print("[Heartbeat] Initiating graceful shutdown...")
        self.shutting_down = True

        self.send_heartbeat()

        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()

        print("[Heartbeat] Shutdown complete")
        sys.exit(0)

    def handle_signal(self, signum, frame):
        self.shutdown()


if __name__ == "__main__":
    heartbeat = Heartbeat()

    signal.signal(signal.SIGINT, heartbeat.handle_signal)
    signal.signal(signal.SIGTERM, heartbeat.handle_signal)

    heartbeat.start()
