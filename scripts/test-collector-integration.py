#!/usr/bin/env python3
"""Unavailable device sources must not stop another Collector pipeline."""
import http.server
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Cloud(http.server.BaseHTTPRequestHandler):
    status_requests = 0
    def do_GET(self):
        if self.path == '/devices':
            payload = {'statusCode': 100, 'body': {'deviceList': [{'deviceId': 'fixture-meter', 'deviceType': 'Meter'}]}}
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())
        else:
            Cloud.status_requests += 1
            self.send_response(401)
            self.end_headers()
    def log_message(self, *args):
        pass


def main():
    binary = str(Path(sys.argv[1] if len(sys.argv) > 1 else './otelcol-extra').resolve())
    cloud = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Cloud)
    threading.Thread(target=cloud.serve_forever, daemon=True).start()
    otlp, telemetry, unavailable = port(), port(), port()
    try:
        with tempfile.TemporaryDirectory(prefix='otelcol-extra-integration-') as folder:
            config = Path(folder) / 'config.yaml'
            config.write_text(f'''receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:{otlp}
  switchbot:
    cloud:
      token: integration-token-fixture
      secret: integration-secret-fixture
      endpoint: http://127.0.0.1:{cloud.server_port}
      collection_interval: 100ms
      min_request_interval: 0s
    battery_interval: 1s
    export_interval: 100ms
    matter:
      enabled: true
      endpoint: ws://127.0.0.1:{unavailable}/ws
      collection_interval: 100ms
      timeout: 100ms
      bindings:
        - device_id: fixture-meter
          node_id: 1
          attributes:
            temperature: 1/1026/0
  smartheim:
    endpoint: http://127.0.0.1:{unavailable}
    timeout: 100ms
    collection_interval: 100ms
exporters:
  debug:
    verbosity: detailed
service:
  telemetry:
    metrics:
      readers:
        - pull:
            exporter:
              prometheus:
                host: 127.0.0.1
                port: {telemetry}
  pipelines:
    metrics/input:
      receivers: [otlp]
      exporters: [debug]
    metrics/switchbot:
      receivers: [switchbot]
      exporters: [debug]
    metrics/smartheim:
      receivers: [smartheim]
      exporters: [debug]
''')
            with (Path(folder) / 'output.log').open('w+') as log:
                process = subprocess.Popen([binary, '--config=' + str(config)], stdout=log, stderr=log)
                try:
                    payload = {'resourceMetrics': [{'scopeMetrics': [{'metrics': [{'name': 'integration.pipeline.alive', 'gauge': {'dataPoints': [{'timeUnixNano': str(time.time_ns()), 'asDouble': 1}]}}]}]}]}
                    deadline = time.monotonic() + 15
                    while True:
                        if process.poll() is not None:
                            raise AssertionError('Collector stopped while a source was unavailable')
                        try:
                            request = urllib.request.Request(f'http://127.0.0.1:{otlp}/v1/metrics', data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
                            with urllib.request.urlopen(request, timeout=1) as response:
                                assert response.status == 200
                            break
                        except (urllib.error.URLError, TimeoutError):
                            if time.monotonic() >= deadline:
                                raise AssertionError('OTLP listener did not start')
                            time.sleep(0.05)
                    time.sleep(1)
                    assert process.poll() is None
                    log.flush()
                    log.seek(0)
                    text = log.read()
                    assert 'integration.pipeline.alive' in text
                    assert 'Smart Heim Navi collection failed' in text
                    assert Cloud.status_requests == 1, '401 cooldown did not bound requests'
                    assert 'integration-token-fixture' not in text
                    assert 'integration-secret-fixture' not in text
                finally:
                    process.terminate()
                    process.wait(timeout=10)
                assert process.returncode == 0
    finally:
        cloud.shutdown()
        cloud.server_close()
    print('Collector pipeline isolation, credential redaction, auth cooldown and shutdown passed')


if __name__ == '__main__':
    main()
