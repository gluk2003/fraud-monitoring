"""Kafka scores -> PostgreSQL, with idempotent inserts and commit-before-offset ordering."""
import logging
import os
import time
from contextlib import closing

import psycopg2
from prometheus_client import start_http_server

from services.metrics import WriterMetrics
from services.stream import consumer

DSN = os.getenv('SCORES_DSN', 'postgresql://fraud:fraud@postgres:5432/fraud')
FIELDS = ('transaction_id', 'transaction_time', 'score', 'fraud_flag', 'model_version',
          'us_state', 'merch', 'cat_id', 'amount', 'scored_at')


def insert_score(db, result):
    with db:
        with db.cursor() as cursor:
            cursor.execute('INSERT INTO scores (' + ','.join(FIELDS) + ') VALUES (' +
                           ','.join(['%s'] * len(FIELDS)) + ') ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id',
                           tuple(result[k] for k in FIELDS))
            inserted = cursor.fetchone() is not None
    return inserted  # context manager has committed by this point


def main():
    metrics = WriterMetrics()
    start_http_server(8001, registry=metrics.registry)
    incoming = consumer('scores', 'fraud-postgres-writer-v4')
    with closing(psycopg2.connect(DSN)) as db:
        for message in incoming:
            start = time.perf_counter()
            try:
                inserted = insert_score(db, message.value)
                metrics.observe(message.value, inserted, time.perf_counter() - start)
                incoming.commit()
            except Exception:
                metrics.errors.inc()
                logging.exception('Write failed; offset remains uncommitted')
                raise


if __name__ == '__main__':
    main()
