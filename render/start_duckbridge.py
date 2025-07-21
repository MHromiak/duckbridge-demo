from duckbridge import *
import signal, os, sys, threading, time

HOST = os.getenv("HOST", "0.0.0.0")
PORT = os.getenv("PORT", "8080")
AUTH = os.getenv("AUTH", "")
DB_PATH = os.getcwd() + "\\" + os.getenv("DB_PATH", "default.db")

bridge = None
shutdown_event = threading.Event()

def start_duckdb_bridge():
    global bridge
    bridge = DuckDBServer()

    print(f"Starting DuckDB server on {HOST}:{PORT} with DB: {DB_PATH}")
    bridge.start(
        path=DB_PATH,
        host=HOST,
        port=PORT,
        readonly=True,
        extension_downloaded=False,
        auth_info=AUTH
    )

def stop_duckdb_bridge(signum, frame):
    global bridge
    print("Shutting down DuckDB server...")
    if bridge:
        try:
            bridge.stop()
        except Exception as e:
            print(f"Warning: Error during shutdown: {e}")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_duckdb_bridge)
    signal.signal(signal.SIGINT, stop_duckdb_bridge)

    start_duckdb_bridge()
    print("Bridge is running. Press Ctrl+C to stop.")

    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_duckdb_bridge(signal.SIGINT, None)

