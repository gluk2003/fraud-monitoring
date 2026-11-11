"""Bounded-cardinality service metrics. Merchant/state analytics live in PostgreSQL."""
from collections import deque
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

SCORE_BUCKETS = tuple(i / 20 for i in range(21))
LATENCY_BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30)


class ScoringMetrics:
    def __init__(self, registry=None):
        self.registry = registry or CollectorRegistry()
        kw = {'registry': self.registry}
        self.transactions = Counter('fraud_scored_total', 'Results acknowledged by Kafka', **kw)
        self.fraud = Counter('fraud_flagged_total', 'Results with fraud_flag=true', **kw)
        self.duration = Histogram('fraud_processing_seconds', 'Inference plus Kafka publish, excluding offset commit', buckets=LATENCY_BUCKETS, **kw)
        self.scores = Histogram('fraud_score', 'Predicted probability', buckets=SCORE_BUCKETS, **kw)
        self.ratio = Gauge('fraud_ratio', 'Fraud fraction in the latest 1000 processed transactions', **kw)
        self.lag = Histogram('fraud_input_age_seconds', 'Time from producer publication to scoring completion', buckets=(.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300), **kw)
        self.last = Gauge('fraud_last_success_timestamp_seconds', 'Unix time of last successful processing', **kw)
        self.errors = Counter('fraud_processing_errors_total', 'Processing failures before restart/retry', **kw)
        self.reloads = Counter('fraud_model_reloads_total', 'Hot replacements, excluding initial load', ['version'], **kw)
        self.reload_time = Gauge('fraud_model_reload_timestamp_seconds', 'Unix time of hot replacement', ['version'], **kw)
        self.active = Gauge('fraud_model_active', 'One for active version, zero for previously active versions', ['version'], **kw)
        self.window = deque(maxlen=1000)
        self.last.set_to_current_time()

    def observe(self, result, duration, age):
        self.transactions.inc()
        self.fraud.inc(int(result['fraud_flag']))
        self.scores.observe(result['score'])
        self.duration.observe(duration)
        self.lag.observe(max(0, age))
        self.window.append(int(result['fraud_flag']))
        self.ratio.set(sum(self.window) / len(self.window))
        self.last.set_to_current_time()


class WriterMetrics:
    def __init__(self, registry=None):
        self.registry = registry or CollectorRegistry()
        kw = {'registry': self.registry}
        self.written = Counter('fraud_db_written_total', 'New unique scores committed to PostgreSQL', **kw)
        self.fraud = Counter('fraud_db_flagged_total', 'New committed flagged scores', **kw)
        self.duration = Histogram('fraud_db_write_seconds', 'SQL insert plus commit, including deduplication attempts', buckets=LATENCY_BUCKETS, **kw)
        self.scores = Histogram('fraud_db_score', 'Scores of new committed rows', buckets=SCORE_BUCKETS, **kw)
        self.duplicates = Counter('fraud_db_duplicates_total', 'Kafka redeliveries ignored by primary key', **kw)
        self.errors = Counter('fraud_db_errors_total', 'Failed write attempts', **kw)

    def observe(self, result, inserted, duration):
        self.duration.observe(duration)
        if inserted:
            self.written.inc()
            self.fraud.inc(int(result['fraud_flag']))
            self.scores.observe(result['score'])
        else:
            self.duplicates.inc()
