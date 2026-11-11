import json
import shutil
import time
from pathlib import Path

import pandas as pd
import pytest

from pipeline import core, tasks
from services import stream
from services.metrics import ScoringMetrics, WriterMetrics


def test_hot_reload_emits_one_event_and_initial_load_is_not_a_reload(tmp_path, monkeypatch):
    for module in (core, tasks, stream):
        monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setenv('SEED_CSV', str(Path(__file__).resolve().parents[1] / 'data/seed.csv'))
    tasks.initialize()
    metrics = ScoringMetrics()
    registry = stream.RegistryModel(metrics)
    row = pd.read_csv(Path(__file__).resolve().parents[1] / 'data/seed.csv').iloc[0].to_dict()
    row['transaction_id'] = 'test'
    first = registry.predict(row)
    assert metrics.registry.get_sample_value('fraud_model_reloads_total', {'version': 'bootstrap'}) is None
    shutil.copyfile(tmp_path / 'models/bootstrap.joblib', tmp_path / 'models/new.joblib')
    core.atomic_json(tmp_path / 'models/latest.json', {**core.manifest(), 'version': 'new', 'file': 'new.joblib'})
    result = registry.predict(row)
    assert result['model_version'] == 'new'
    assert result['score'] == first['score']
    registry.predict(row)
    assert metrics.registry.get_sample_value('fraud_model_reloads_total', {'version': 'new'}) == 1
    assert metrics.registry.get_sample_value('fraud_model_active', {'version': 'bootstrap'}) == 0
    assert metrics.registry.get_sample_value('fraud_model_active', {'version': 'new'}) == 1
    assert time.time() - metrics.registry.get_sample_value('fraud_model_reload_timestamp_seconds', {'version': 'new'}) < 30
    # Bad artifact does not replace the loaded model or emit a successful reload.
    core.atomic_json(tmp_path / 'models/latest.json', {'version': 'broken', 'file': 'missing.joblib'})
    with pytest.raises(FileNotFoundError):
        registry.predict(row)
    assert registry.version == 'new'
    assert metrics.registry.get_sample_value('fraud_model_reloads_total', {'version': 'broken'}) is None


def test_writer_counts_only_new_committed_rows():
    metrics = WriterMetrics()
    row = {'score': .9, 'fraud_flag': True}
    metrics.observe(row, inserted=True, duration=.01)
    metrics.observe(row, inserted=False, duration=.02)
    assert metrics.registry.get_sample_value('fraud_db_written_total') == 1
    assert metrics.registry.get_sample_value('fraud_db_flagged_total') == 1
    assert metrics.registry.get_sample_value('fraud_db_duplicates_total') == 1
    assert metrics.registry.get_sample_value('fraud_db_score_count') == 1
    assert metrics.registry.get_sample_value('fraud_db_write_seconds_count') == 2


def test_rolling_ratio_and_input_age():
    metrics = ScoringMetrics()
    for _ in range(1000):
        metrics.observe({'score': .9, 'fraud_flag': True}, .01, .5)
    for _ in range(1000):
        metrics.observe({'score': .1, 'fraud_flag': False}, .02, -1)
    assert metrics.registry.get_sample_value('fraud_ratio') == 0
    assert metrics.registry.get_sample_value('fraud_scored_total') == 2000
    assert metrics.registry.get_sample_value('fraud_input_age_seconds_sum') == 500
