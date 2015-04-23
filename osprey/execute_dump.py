from __future__ import print_function, absolute_import, division

import csv
import json
import pandas as pd
from six.moves import cStringIO
from .config import Config
from .trials import Trial


def execute(args, parser):
    config = Config(args.config, verbose=False)

    session = config.trials()
    columns = Trial.__mapper__.columns

    if args.output == 'json':
        items = [curr.to_dict() for curr in session.query(Trial).all()]
        value = json.dumps(items)

    elif args.output == 'csv':
        buf = cStringIO()
        outcsv = csv.writer(buf)
        outcsv.writerow([column.name for column in columns])
        for curr in session.query(Trial).all():
            row = [getattr(curr, column.name) for column in columns]
            outcsv.writerow(row)
        value = buf.getvalue()

    elif args.output == 'best':
        with config.trialscontext() as session:
            q = (session.query(Trial)
                 .filter(Trial.status == 'SUCCEEDED')
                 .order_by(Trial.started))
            data = [curr.to_dict() for curr in q.all()]
        df_all = pd.DataFrame(data)
        top = df_all.sort('mean_test_score', ascending=False)[:10]
        print(top[['mean_test_score', 'parameters']])
        t = top.iloc[0]['parameters']
        value = pd.Series(t)

    print(value)
    return value


def nonconstant_parameters(data):
    assert len(data) > 0
    df = pd.DataFrame([d['parameters'] for d in data])
    # http://stackoverflow.com/a/20210048/1079728
    filtered = df.loc[:, (df != df.ix[0]).any()]
    return filtered
