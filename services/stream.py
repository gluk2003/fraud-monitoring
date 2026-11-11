"""Continuous Kafka input and CPU scorer. Offsets commit only after acknowledged output."""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import start_http_server

from pipeline.core import ROOT, features, manifest
from services.metrics import ScoringMetrics

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
BROKERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


class RegistryModel:
    def __init__(self, metrics=None):
        self.version = None
        self.model = None
        self.metrics = metrics

    def predict(self, row):
        current = manifest()
        if current['version'] != self.version:
            candidate = joblib.load(ROOT / 'models' / current['file'])
            previous = self.version
            self.model, self.version = candidate, current['version']
            if self.metrics:
                if previous is not None:
                    self.metrics.active.labels(version=previous).set(0)
                    self.metrics.reloads.labels(version=self.version).inc()
                    self.metrics.reload_time.labels(version=self.version).set_to_current_time()
                self.metrics.active.labels(version=self.version).set(1)
            log.info('Loaded model %s (previous=%s)', self.version, previous)
        score = float(self.model.predict_proba(features(pd.DataFrame([row])))[0, 1])
        # Existing HW3 fields are preserved; business dimensions are additive.
        return {
            'transaction_id': row['transaction_id'], 'transaction_time': row['transaction_time'],
            'score': score, 'fraud_flag': score >= .5, 'model_version': self.version,
            'us_state': row['us_state'], 'merch': row['merch'], 'cat_id': row['cat_id'],
            'amount': float(row['amount']), 'scored_at': datetime.now(timezone.utc).isoformat(),
        }


def producer():
    return KafkaProducer(bootstrap_servers=BROKERS, value_serializer=lambda v: json.dumps(v).encode(), acks='all', retries=10)


def consumer(topic, group):
    return KafkaConsumer(topic, bootstrap_servers=BROKERS, group_id=group,
                         auto_offset_reset='earliest', enable_auto_commit=False,
                         value_deserializer=lambda b: json.loads(b), max_poll_records=1,
                         max_poll_interval_ms=300000)


def simulate():
    rows = pd.read_csv('/app/data/seed.csv').drop(columns=['target']).to_dict('records')
    out = producer()
    rng = random.Random(42)
    while True:
        control = ROOT / 'demo.json'
        config = json.loads(control.read_text()) if control.exists() else {}
        row = dict(config.get('row') or rng.choice(rows))
        row['transaction_id'] = str(uuid.uuid4())
        # Preserve event time for training/inference parity; produced_at is live time.
        row['produced_at'] = time.time()
        out.send('transactions', row).get(timeout=30)
        time.sleep(float(os.getenv('STREAM_INTERVAL', '.2')))


def score():
    metrics = ScoringMetrics()
    start_http_server(8000, registry=metrics.registry)
    incoming, out = consumer('transactions', 'fraud-scorer-v4'), producer()
    registry = RegistryModel(metrics)
    for message in incoming:
        start = time.perf_counter()
        try:
            result = registry.predict(message.value)
            out.send('scores', result).get(timeout=30)
            metrics.observe(result, time.perf_counter() - start,
                            time.time() - float(message.value.get('produced_at', time.time())))
            incoming.commit()
        except Exception:
            metrics.errors.inc()
            log.exception('Processing failed; offset remains uncommitted')
            raise


if __name__ == '__main__':
    {'simulate': simulate, 'score': score}[os.environ['STREAM_ROLE']]()
