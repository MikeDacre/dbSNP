#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build the database without SQLAlchemy for speed.

Example
-------
>>> import dbSNP
>>> db = dbSNP.DB('/path/to/db', version=142)
>>> db.inititialize_db()
>>> for bed_file in [os.path.join(os.abspath('.'), i)
                     for i in os.listdir('.') if i.endswith('.bed')]:
        dbSNP.build_db('/path/to/db/dbSNP142.db', bed_file)
"""
import sqlite3
from subprocess import check_output as _check_output
from tqdm import tqdm


def build_db(db, f, commit_every=1000000):
    """Initialize the database, must exist already.

    Parameters
    ----------
    f : str
        File to read.
    commit_every : int, optional
        How many rows to wait before commiting.
    """
    rows    = 0
    count   = commit_every

    db_len  = int(
        _check_output('wc -l {}'.format(f), shell=True)
        .decode().strip().split(' ')[0]
    )
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = 500000")
    records = []
    with open(f) as fin, tqdm(unit='rows', total=db_len) as pbar:
        for line in fin:
            if line.startswith('track'):
                continue
            chrom, start, end, name, _, strand = line.rstrip().split('\t')
            records.append(
                (name, chrom, start, end, strand)
            )
            if count:
                count -= 1
            else:
                pbar.write('Writing {} records...'.format(commit_every))
                conn.executemany(
                    'insert into dbSNP(name, chrom, start, end, strand) ' +
                    'values (?, ?, ?, ?, ?)', records
                )
                pbar.write('Written')
                count = commit_every-1
                records = []
            rows += 1
            pbar.update()
    print('Writing final rows')
    conn.executemany(
        'insert into dbSNP(name, chrom, start, end, strand) ' +
        'values (?, ?, ?, ?, ?)', records
    )
    conn.commit()
    # Add/Update length
    print('Adding length.')
    dblen = int(conn.execute('SELECT COUNT(*) FROM dbSNP;').fetchone()[0])
    conn.execute(
        """\
        INSERT OR REPLACE INTO dbInfo (name, value) \
            VALUES ('length', '{}');\
        """.format(dblen)
    )
    conn.commit()
    conn.close()
    print('Done, {} rows written'.format(rows))
