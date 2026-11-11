"""Exercise alerts using real transactions and inference, never synthetic metric values."""
import argparse
import json
import pandas as pd
import joblib
from pipeline.core import ROOT, atomic_json, features, manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['normal', 'high-fraud'])
    mode = parser.parse_args().mode
    if mode == 'normal':
        atomic_json(ROOT / 'demo.json', {})
    else:
        current = manifest()
        model = joblib.load(ROOT / 'models' / current['file'])
        df = pd.read_csv('/app/data/seed.csv')
        scores = model.predict_proba(features(df))[:, 1]
        i = int(scores.argmax())
        if scores[i] < .5:
            raise RuntimeError('Current model cannot produce a flagged demo transaction')
        row = df.iloc[i].drop(labels=['target']).to_dict()
        atomic_json(ROOT / 'demo.json', {'row': row})
        print(json.dumps({'mode': mode, 'score': float(scores[i]), 'model_version': current['version']}))


if __name__ == '__main__':
    main()
