#!/usr/bin/env python3
"""Exercise weather outbox replay, corrections and dedup through real OTLP HTTP."""
import http.server
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse


ROOT = Path(__file__).resolve().parents[1]
FORECAST = json.loads((ROOT / 'receiver/openmeteoreceiver/testdata/forecast.json').read_text())
JMA = (ROOT / 'receiver/jmareceiver/testdata/kofu-hourly.html').read_text()
AMEDAS = (ROOT / 'receiver/jmareceiver/testdata/katsunuma-hourly.html').read_text()
STATE_NAMES = ('openmeteo', 'jma', 'jma-amedas')


class Server(http.server.BaseHTTPRequestHandler):
    lock = threading.Lock()
    accepting = False
    correction = False
    attempted = set()
    delivered = []

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path == '/forecast':
            with self.lock:
                data = json.loads(json.dumps(FORECAST))
                if self.correction:
                    data['hourly']['shortwave_radiation'][0] = None
            body = json.dumps(data).encode()
        else:
            query = urllib.parse.parse_qs(url.query)
            date = f"{query['year'][0]}年{query['month'][0]}月{query['day'][0]}日"
            table = AMEDAS if query['block_no'][0] == '0433' else JMA
            body = table.replace('2026年10月24日', date, 1).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        events = [json.loads(record['body']['stringValue'])
                  for resource in payload['resourceLogs']
                  for scope in resource['scopeLogs']
                  for record in scope['logRecords']]
        with self.lock:
            self.attempted.update((e['source'], e['location_id']) for e in events)
            accepting = self.accepting
            if accepting:
                self.delivered.extend(events)
        self.send_response(200 if accepting else 503)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{}')

    def log_message(self, *args):
        pass


def wait_for(predicate, process, label):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError('Collector exited during ' + label)
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError('Timed out: ' + label)


def main():
    binary = str(Path(sys.argv[1] if len(sys.argv) > 1 else './otelcol-extra').resolve())
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix='weather-integration-') as folder:
            folder = Path(folder)
            endpoint = f'http://127.0.0.1:{server.server_port}'
            config = folder / 'config.yaml'
            config.write_text(f'''receivers:
  openmeteo:
    endpoint: {endpoint}/forecast
    latitude: 35.6667
    longitude: 138.55
    location_id: fixture
    state_file: {folder}/openmeteo.json
    collection_interval: 200ms
  jma:
    endpoint: {endpoint}/hourly
    prec_no: "49"
    block_no: "47638"
    station_name: 甲府
    state_file: {folder}/jma.json
    collection_interval: 200ms
    recent_days: 1
    revision_days: 1
    min_request_interval: 0s
  jma/amedas:
    endpoint: {endpoint}/hourly
    prec_no: "49"
    block_no: "0433"
    station_name: 勝沼
    state_file: {folder}/jma-amedas.json
    collection_interval: 200ms
    recent_days: 1
    revision_days: 1
    min_request_interval: 0s
exporters:
  otlp_http:
    endpoint: {endpoint}
    encoding: json
    compression: none
    sending_queue:
      enabled: false
    retry_on_failure:
      enabled: false
service:
  telemetry:
    metrics:
      level: none
  pipelines:
    logs:
      receivers: [openmeteo, jma, jma/amedas]
      exporters: [otlp_http]
''')
            with (folder / 'collector.log').open('w+') as log:
                def start():
                    return subprocess.Popen([binary, '--config=' + str(config)], stdout=log, stderr=log)

                def stop(process):
                    process.terminate()
                    process.wait(timeout=10)
                    assert process.returncode == 0

                process = start()
                try:
                    wait_for(lambda: Server.attempted == {('openmeteo', 'fixture'), ('jma', '47638'), ('jma', '0433')}, process, 'failed export from all receiver instances')
                finally:
                    stop(process)
                pending = []
                for source in STATE_NAMES:
                    state = json.loads((folder / f'{source}.json').read_text())
                    assert state['pending'], source + ' dropped its failed export'
                    pending.extend(state['pending'])
                ids = {e['event_id'] for e in pending}
                with Server.lock:
                    Server.accepting = True
                process = start()
                try:
                    wait_for(lambda: ids <= {e['event_id'] for e in Server.delivered}, process, 'durable replay')
                    with Server.lock:
                        Server.correction = True
                    wait_for(lambda: any(e['revision'] == 2 and e['value'] is None for e in Server.delivered), process, 'withdrawal revision')
                    time.sleep(0.7)
                finally:
                    stop(process)
                unique = {e['event_id']: e for e in Server.delivered}
                assert len(unique) == len(ids) + 1, 'unchanged values created extra events'
                revision = next(e for e in unique.values() if e['revision'] == 2)
                assert revision['quality'] == 'missing' and revision['supersedes'] in ids
                amedas = [e for e in unique.values() if e['location_id'] == '0433']
                assert len(amedas) == 72
                assert {e['variable'] for e in amedas} == {'precipitation', 'air_temperature', 'sunshine_duration'}
                assert all(e['kind'] == 'estimate' for e in amedas if e['variable'] == 'sunshine_duration')
                assert all(not json.loads((folder / f'{s}.json').read_text()).get('pending') for s in STATE_NAMES)
                count = len(Server.delivered)
                process = start()
                try:
                    time.sleep(0.7)
                    assert process.poll() is None
                    assert len(Server.delivered) == count, 'restart emitted unchanged observations'
                finally:
                    stop(process)
    finally:
        server.shutdown()
        server.server_close()
    print('Weather OTLP replay, revisions, deduplication and simultaneous JMA station/AMeDAS instances passed')


if __name__ == '__main__':
    main()
