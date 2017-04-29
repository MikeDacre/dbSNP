# -*- coding: utf-8 -*-
"""
Python SQLAlchemy Interface for dbSNP
"""
import os as _os
from random import randint as _rand
from subprocess import check_output as _check_output

from multiprocessing import Pool as _Pool
from multiprocessing import cpu_count as _cpu_count

from numpy import array_split as _array_split

from tqdm import tqdm as _tqdm

import sqlalchemy as sa

from sqlalchemy.ext.declarative import declarative_base as _base
from sqlalchemy.orm import sessionmaker as _sessionmaker

import pandas as _pd


class DB(object):

    """A class for interacting with the database."""

    Base = _base()
    _length = None

    class Row(Base):

        """A simple SQLAlchemy dbSNP interface."""

        __tablename__ = 'dbSNP'

        id     = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
        name   = sa.Column(sa.String, unique=True, index=True)
        chrom  = sa.Column(sa.String(length=4), index=True)
        start  = sa.Column(sa.Integer, index=True)
        end    = sa.Column(sa.Integer, index=True)
        strand = sa.Column(sa.String(length=1), index=True)

        sa.Index('chrom_start_index', chrom, start)

        def __repr__(self):
            """Display simple information about the row."""
            return '{name}<{chrom}:{start}-{end}>'.format(
                name=self.name, chrom=self.chrom,
                start=self.start, end=self.end
            )

    class Info(Base):

        """Information about the database, currently only length."""

        __tablename__ = 'dbInfo'

        id    = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
        name  = sa.Column(sa.String, unique=True)
        value = sa.Column(sa.String)

    def __init__(self, location, version=150):
        """Connect to the database.

        Parameters
        ----------
        location : str
            A location that describes the directory where the database lives.
            This will usually be just a directory path, but it can also be a
            full sqlite connect string, in which case a remote location can
            be specified.
            This should not include the database file, that will be
            automatically constructed using the version parameter.
        version : int, optional
            A dbSNP version, used to pick the database file.

        Raises
        ------
        ValueError
            If the location is not found.
        """
        self.version = int(version)
        if location.endswith('.db'):
            raise ValueError('Do not specify the database by file location, ' +
                             'use directory and version.')
        if not location.startswith('sqlite:'):
            self.location = _os.path.abspath(location)
            if not _os.path.isdir(self.location):
                raise ValueError('{} is not a directory'.format(location))
            location = 'sqlite:///{}'.format(location)
        self.file = location.rstrip('/') + '/dbsnp{}.db'.format(version)
        self.engine = sa.create_engine(self.file)

    @property
    def exists(self):
        """Check if the file exists."""
        return _os.path.isfile(self.file)

    def __len__(self):
        """Return database length, only lookup once per DB instance."""
        if not self._length:
            self._length = int(
                self.query(self.Info.value)
                .filter(self.Info.name == 'length')
                .first()[0]
            )
        return self._length

    def __repr__(self):
        """String representation of DB."""
        return "dbSNP<Version={},Length={}>".format(self.version, len(self))

    def session(self):
        """Return a session for this database."""
        return _sessionmaker(bind=self.engine)()

    def query(self, *args, **kwargs):
        """Create a pre-initialized query."""
        if not args:
            args = (self.Row,)
        session = self.get_session()
        return session.query(*args, **kwargs)

    def random(self, count=1, as_df=False):
        """Return a random record from the db."""
        if count > 1:
            r = [_rand(1, len(self)) for i in range(count)]
            q = self.query().filter(self.Row.id.in_(r))
        else:
            q = self.query().filter(self.Row.id == _rand(1, len(self)))
        if as_df:
            return _pd.read_sql_query(q.statement, self.engine)
        else:
            return q.all()

    def lookup_rsids(self, rsids: list([str])) -> list([Row]):
        """Return either one row or a list of rows by rsID.

        Parameters
        ----------
        rsids : list_of_str or str

        Returns
        -------
        list
            A list of DB.Row objects
        """
        if isinstance(rsids, str):
            rsids = [rsids]
        for i in rsids:
            assert isinstance(i, str)
            assert i.startswith('rs')
        return self.query().filter(self.Row.name.in_(rsids)).all()

    def lookup_location(self, chrom: str, start: int, end: int=None) -> Row:
        """Return a row by location.

        Only does one at a time, optimized for speed.

        Parameters
        ----------
        chrom : str
        start : int
        end : int, optional

        Returns
        -------
        DB.Row
        """
        if not isinstance(chrom, (str, int)):
            raise ValueError('chrom must be int or string')
        chrom = str(chrom)
        start = int(start)
        if end:
            end = int(end)
        if not chrom.startswith('chr'):
            chrom = 'chr' + chrom
        query =  self.query().filter(
            self.Row.chrom == chrom
        ).filter(
            self.Row.start == start
        )
        if end:
            query = query.filter(self.Row.end == end)
        query = query.with_hint(self.Row, 'USE INDEX chrom_start_index')
        return query.first()

    def lookup_locations(self, locs: dict(str=list)) -> list:
        """Return a row by location.

        One query for every chromosome.

        Parameters
        ----------
        loct : dict
            In format: {chrom: list of starts}

        Returns
        -------
        list of DB.Row
        """
        if not isinstance(list(locs.keys())[0], (str, int)):
            raise ValueError('chrom items must be int or string')
        c = {}
        for i in locs.keys():
            o = locs[i]
            i = str(i)
            if not i.startswith('chr'):
                i = 'chr' + i
            s = []
            for j in o:
                s.append(int(j))
            c[i] = s
        locs = c

        results = []
        for chrom in locs:
            query =  self.query().filter(
                self.Row.chrom == chrom
            ).filter(
                self.Row.start.in_(locs[chrom])
            )
            query = query.with_hint(self.Row, 'USE INDEX chrom_start_index')
            results += query.all()
        return results

    def initialize_db(self):
        """Initialize the database, must exist already.

        Args:
            f (str): File to read.
            commit_every (int): How many rows to wait before commiting.
        """

        a = input('Create a new database (will destroy existing one)? [y/N] ')
        if not a.upper().startswith('Y'):
            return

        if not self.file.startswith('sqlite:///'):
            raise Exception('Cannot initialize a remote database ' +
                            '(specify the database by location to avoid this)')

        if _os.path.isfile(self.file):
            _os.remove(self.file)

        # Create db tables, must be deleted first
        self.Base.metadata.create_all(self.engine)

    def build_db(self, f, commit_every=1000000):
        """Initialize the database, must exist already.

        Args:
            f (str): File to read.
            commit_every (int): How many rows to wait before commiting.
        """
        rows    = 0
        count   = commit_every
        dbsnp   = self.Row.__table__
        insert  = dbsnp.insert()

        db_len  = int(
            _check_output('wc -l {}'.format(f), shell=True)
            .decode().strip().split(' ')[0]
        )
        conn = self.engine.connect()
        records = []
        with open(f) as fin, _tqdm(unit='rows', total=db_len) as pbar:
            for line in fin:
                if line.startswith('track'):
                    continue
                chrom, start, end, name, _, strand = line.rstrip().split('\t')
                records.append(
                    {'name': name, 'chrom': chrom, 'start': start,
                     'end': end, 'strand': strand}
                )
                if count:
                    count -= 1
                else:
                    pbar.write('Writing {} records...'.format(commit_every))
                    pbar.update(0)
                    conn.execute(insert, records)
                    pbar.write('Written, {} complete'.format(rows))
                    count = commit_every-1
                    records = []
                rows += 1
                pbar.update()
        print('Writing final rows')
        conn.execute(insert, records)
        conn.close()
        print('Done, {} rows written'.format(rows))
