"""Generate our dashboard JSON and alert provisioning. No seminar dashboard is imported."""
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
PROM = {'type': 'prometheus', 'uid': 'prometheus'}
PG = {'type': 'grafana-postgresql-datasource', 'uid': 'postgres'}
FILTER = "('__all' IN (${state:sqlstring}) OR us_state IN (${state:sqlstring})) AND ('__all' IN (${merchant:sqlstring}) OR merch IN (${merchant:sqlstring}))"


def prom(expr, legend=''):
    return {'refId': 'A', 'expr': expr, 'legendFormat': legend, 'range': True, 'datasource': PROM}


def sql(query, timeseries=False):
    return {'refId': 'A', 'rawSql': query, 'format': 'time_series' if timeseries else 'table',
            'rawQuery': True, 'editorMode': 'code', 'datasource': PG}


def panel(title, target, unit='short', kind='timeseries', description=''):
    if kind == 'stat' and target['datasource'] == PROM:
        target = {**target, 'instant': True, 'range': False}
    return {'title': title, 'type': kind, 'datasource': target['datasource'], 'targets': [target],
            'description': description, 'fieldConfig': {'defaults': {'unit': unit, 'min': 0,
            'color': {'mode': 'palette-classic'}, 'custom': {'drawStyle': 'line', 'fillOpacity': 12}}, 'overrides': []},
            'options': {'legend': {'displayMode': 'list', 'placement': 'bottom'}, 'tooltip': {'mode': 'multi'}}}


def heatmap(title, metric):
    p = panel(title, prom(f'sum by (le) (rate({metric}_bucket[$__rate_interval]))', '{{le}}'), kind='heatmap')
    p['targets'][0]['format'] = 'heatmap'
    p['options'] = {'calculate': False, 'cellGap': 1, 'color': {'mode': 'scheme', 'scheme': 'Oranges', 'steps': 64},
                    'yAxis': {'axisLabel': 'Score', 'unit': 'short'}, 'legend': {'show': True},
                    'tooltip': {'show': True, 'yHistogram': True}}
    return p


def dashboard(uid, title, panels, variables=None, annotations=None):
    for i, p in enumerate(panels):
        p.update(id=i + 1, gridPos={'x': i % 2 * 12, 'y': i // 2 * 9, 'w': 12, 'h': 9})
    value = {'uid': uid, 'title': title, 'schemaVersion': 40, 'version': 1, 'editable': True,
             'tags': ['hw4', 'fraud'], 'timezone': 'browser', 'refresh': '2s',
             'time': {'from': 'now-15m', 'to': 'now'}, 'panels': panels,
             'templating': {'list': variables or []}, 'annotations': {'list': annotations or []},
             'links': [{'title': 'Scoring', 'url': '/d/fraud-scoring'}, {'title': 'Writer', 'url': '/d/fraud-writer'},
                       {'title': 'System', 'url': '/d/fraud-system'}, {'title': 'Business', 'url': '/d/fraud-business'}]}
    (BASE / 'grafana/dashboards' / f'{uid}.json').write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def variable(name, label, query):
    return {'name': name, 'label': label, 'type': 'query', 'datasource': PG,
            'query': query, 'definition': query, 'refresh': 1, 'multi': True, 'includeAll': True,
            # Grafana does not apply :sqlstring to a custom All value.
            'allValue': "'__all'", 'sort': 1, 'current': {'text': 'All', 'value': '$__all'}}


def main():
    annotations = [{'name': 'Model hot reload (30 seconds)', 'enable': True, 'iconColor': '#FF9830',
        'datasource': PROM, 'expr': 'fraud_model_reloads_total > 0 and on(version) (time() - fraud_model_reload_timestamp_seconds < 30)',
        'step': '1s', 'titleFormat': 'Loaded {{version}}', 'textFormat': 'New model activated without restarting scorer', 'tagKeys': 'version'}]
    dashboard('fraud-scoring', '01 · Скоринг потока', [
        panel('Скорость скоринга · TPS', prom('rate(fraud_scored_total[$__rate_interval])'), 'ops'),
        panel('Задержка обработки · p50 / p95', prom('histogram_quantile(0.95, sum by(le) (rate(fraud_processing_seconds_bucket[$__rate_interval])))', 'p95'), 's'),
        panel('Доля фрода · за последнюю минуту', prom('rate(fraud_flagged_total[1m]) / clamp_min(rate(fraud_scored_total[1m]), 0.001)'), 'percentunit'),
        heatmap('Распределение скоров во времени', 'fraud_score'),
        panel('Возраст сообщения · p95', prom('histogram_quantile(0.95, sum by(le) (rate(fraud_input_age_seconds_bucket[$__rate_interval])))'), 's', description='Собственная метрика: возраст от отправки продюсером до завершения скоринга. Выявляет очередь/отставание Kafka при быстром inference.'),
        panel('Активная версия модели', prom('fraud_model_active == 1', '{{version}}'), kind='stat'),
        panel('Секунд с последней транзакции', prom('time() - fraud_last_success_timestamp_seconds'), 's'),
        panel('Ошибки обработки · за минуту', prom('increase(fraud_processing_errors_total[1m])')),
    ], annotations=annotations)
    # Add the second quantile to the latency panel.
    path = BASE / 'grafana/dashboards/fraud-scoring.json'
    d = json.loads(path.read_text())
    d['panels'][1]['targets'].append({**prom('histogram_quantile(0.50, sum by(le) (rate(fraud_processing_seconds_bucket[$__rate_interval])))', 'p50'), 'refId': 'B'})
    d['panels'][5]['options'] = {'textMode': 'name', 'graphMode': 'none', 'colorMode': 'value',
                               'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}
    path.write_text(json.dumps(d, ensure_ascii=False, indent=2) + '\n')

    dashboard('fraud-writer', '02 · Запись результатов', [
        panel('Скорость записи · TPS', prom('rate(fraud_db_written_total[$__rate_interval])'), 'ops'),
        panel('Запись и commit · p95', prom('histogram_quantile(0.95, sum by(le) (rate(fraud_db_write_seconds_bucket[$__rate_interval])))'), 's'),
        panel('Записано уникальных скоров', prom('fraud_db_written_total'), kind='stat'),
        panel('Выявлено фрода · после запуска writer', prom('fraud_db_flagged_total'), kind='stat'),
        heatmap('Распределение записанных скоров', 'fraud_db_score'),
        panel('Дубли и ошибки · за минуту', prom('increase(fraud_db_duplicates_total[1m])', 'duplicates')),
        panel('Результаты по версии модели · PostgreSQL', sql('SELECT model_version, count(*) AS scores, avg(fraud_flag::int) AS fraud_ratio, avg(score) AS avg_score FROM scores WHERE $__timeFilter(scored_at) GROUP BY model_version ORDER BY model_version'), kind='table', description='Персистентные данные PostgreSQL в выбранном временном диапазоне, включая предыдущие запуски writer.'),
        panel('Ошибки записи · за минуту', prom('increase(fraud_db_errors_total[1m])')),
    ])
    dashboard('fraud-system', '03 · Системные метрики', [
        panel('CPU · занято', prom('1 - avg(rate(node_cpu_seconds_total{mode="idle"}[$__rate_interval]))'), 'percentunit'),
        panel('RAM · занято', prom('1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes'), 'percentunit'),
        panel('Сеть · входящие байты', prom('rate(node_host_network_receive_bytes_total{device!~"lo|veth.*|br-.*|docker.*"}[$__rate_interval])', '{{device}}'), 'Bps'),
        panel('Сеть · исходящие байты', prom('rate(node_host_network_transmit_bytes_total{device!~"lo|veth.*|br-.*|docker.*"}[$__rate_interval])', '{{device}}'), 'Bps'),
        panel('Диск · занято', prom('1 - node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs",mountpoint="/"} / node_filesystem_size_bytes{fstype!~"tmpfs|overlay|squashfs",mountpoint="/"}'), 'percentunit'),
        panel('Диск · чтение / запись', prom('rate(node_disk_written_bytes_total[$__rate_interval])', '{{device}} write'), 'Bps'),
    ])
    path = BASE / 'grafana/dashboards/fraud-system.json'
    d = json.loads(path.read_text())
    d['panels'][5]['targets'].append({**prom('rate(node_disk_read_bytes_total[$__rate_interval])', '{{device}} read'), 'refId': 'B'})
    path.write_text(json.dumps(d, ensure_ascii=False, indent=2) + '\n')

    density = f'''WITH selected AS (SELECT least(19, floor(score * 20)::int) AS bin FROM scores
        WHERE $__timeFilter(scored_at) AND {FILTER}),
        counts AS (SELECT bin, count(*) AS n FROM selected GROUP BY bin)
        SELECT to_char(b / 20.0, 'FM0.00') || '–' || to_char((b+1) / 20.0, 'FM0.00') AS score_bin,
        coalesce(n, 0)::float / greatest((SELECT count(*) FROM selected), 1) / 0.05 AS density
        FROM generate_series(0,19) b LEFT JOIN counts ON bin=b ORDER BY b'''
    latest = f'''WITH last_1000 AS (SELECT * FROM scores ORDER BY scored_at DESC, transaction_id DESC LIMIT 1000)
        SELECT cat_id, avg(fraud_flag::int) AS fraud_ratio FROM last_1000 WHERE {FILTER}
        GROUP BY cat_id ORDER BY fraud_ratio DESC'''
    business = [
        panel('Плотность распределения скоров · state / merch', sql(density), kind='barchart', description='20 интервалов ширины 0.05. Высота=count/(N×0.05), площадь равна 1 при N>0. Последний интервал включает score=1. Фильтры применяются ко всему выбранному времени.'),
        panel('TPS обработки · state / merch', sql(f'SELECT $__timeGroupAlias(scored_at,\'5s\',0), count(*) / 5.0 AS tps FROM scores WHERE $__timeFilter(scored_at) AND {FILTER} GROUP BY 1 ORDER BY 1', True), 'ops', description='Время фактического скоринга, интервалы 5 секунд. SQL видит результаты после записи writer.'),
        panel('Доля фрода по категории · последние 1000', sql(latest), 'percentunit', kind='barchart', description='Последние 1000 уникальных результатов глобально, затем фильтры state/merch. Диапазон времени не ограничивает это окно; пока строк меньше 1000, используется доступное количество.'),
        panel('Топ продавцов по сумме подозрительных покупок', sql(f'SELECT merch, sum(amount) FILTER (WHERE fraud_flag) AS flagged_amount FROM scores WHERE $__timeFilter(scored_at) AND {FILTER} GROUP BY merch HAVING bool_or(fraud_flag) ORDER BY flagged_amount DESC LIMIT 10'), kind='barchart', description='Дополнительный бизнес-график: помогает приоритизировать расследования по объёму подозрительных покупок. Это предсказания модели, не подтверждённые потери.'),
        panel('Скоры и доля фрода по версии · выбранный сегмент', sql(f'SELECT model_version, count(*) AS scores, avg(fraud_flag::int) AS fraud_ratio, sum(amount) FILTER (WHERE fraud_flag) AS flagged_amount FROM scores WHERE $__timeFilter(scored_at) AND {FILTER} GROUP BY model_version ORDER BY model_version'), kind='table'),
        panel('Размер сегмента', sql(f'SELECT count(*) AS scores FROM scores WHERE $__timeFilter(scored_at) AND {FILTER}'), kind='stat'),
    ]
    for p in business:
        if p['type'] == 'barchart':
            p['options'] = {'orientation': 'auto', 'showValue': 'auto', 'groupWidth': .85, 'barWidth': .9,
                            'legend': {'showLegend': False}, 'tooltip': {'mode': 'single'}, 'xTickLabelRotation': -30}
    dashboard('fraud-business', '04 · Бизнес и сегменты', business, [
        variable('state', 'Штат · us_state', 'SELECT DISTINCT us_state AS __text, us_state AS __value FROM scores ORDER BY 1'),
        variable('merchant', 'Продавец · merch', "SELECT DISTINCT merch AS __text, merch AS __value FROM scores WHERE ('__all' IN (${state:sqlstring}) OR us_state IN (${state:sqlstring})) ORDER BY 1"),
    ])
    rules = []
    for uid, title, expr, threshold, panel_id in [
        ('fraud-high-ratio', 'Fraud ratio above 20%', 'sum(rate(fraud_flagged_total[1m])) / clamp_min(sum(rate(fraud_scored_total[1m])), 0.001)', .2, 3),
        ('fraud-stopped-stream', 'No successful scoring for 20 seconds', 'time() - max(fraud_last_success_timestamp_seconds)', 20, 7),
    ]:
        rules.append({'uid': uid, 'title': title, 'condition': 'B', 'for': '10s',
            'noDataState': 'Alerting', 'execErrState': 'Alerting',
            'labels': {'service': 'scorer', 'severity': 'warning'},
            'annotations': {'summary': title, '__dashboardUid__': 'fraud-scoring', '__panelId__': str(panel_id)},
            'data': [
                {'refId': 'A', 'datasourceUid': 'prometheus', 'relativeTimeRange': {'from': 120, 'to': 0},
                 'model': {'refId': 'A', 'expr': expr, 'instant': True, 'range': False, 'intervalMs': 1000,
                           'maxDataPoints': 43200, 'datasource': PROM}},
                {'refId': 'B', 'datasourceUid': '__expr__', 'relativeTimeRange': {'from': 0, 'to': 0},
                 'model': {'refId': 'B', 'type': 'threshold', 'expression': 'A',
                           'datasource': {'type': '__expr__', 'uid': '__expr__'},
                           'conditions': [{'type': 'query', 'evaluator': {'type': 'gt', 'params': [threshold]},
                                           'reducer': {'type': 'last', 'params': []}, 'operator': {'type': 'and'}, 'query': {'params': ['B']}}]}},
            ]})
    config = {'apiVersion': 1, 'groups': [{'orgId': 1, 'name': 'fraud-service', 'folder': 'Fraud Monitoring', 'interval': '10s', 'rules': rules}],
        'muteTimes': [{'orgId': 1, 'name': 'local-status-only', 'time_intervals': [
            {'times': [{'start_time': '00:00', 'end_time': '24:00'}], 'location': 'UTC'}]}],
        'policies': [{'orgId': 1, 'receiver': 'grafana-default-email',
            'group_by': ['grafana_folder', 'alertname'], 'routes': [
                {'receiver': 'grafana-default-email', 'object_matchers': [['service', '=', 'scorer']],
                 'mute_time_intervals': ['local-status-only']}]}]}
    (BASE / 'grafana/provisioning/alerting/rules.json').write_text(json.dumps(config, indent=2) + '\n')


if __name__ == '__main__':
    main()
