# -*- coding: utf-8 -*-
"""
Python SQLAlchemy Interface for dbSNP
"""
import os as _os
from random import randint as _rand
from subprocess import check_output as _check_output

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

        @property
        def length(self):
            """Calculate the length of self."""
            return self.end - self.start

        def __len__(self):
            """Return the length of self."""
            return self.length

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
        return abs(int(self._length))

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
        session = self.session()
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

    def between(self, locations, snps_only=False):
        """Return all recordss in a given window.

        Parameters
        ----------
        locations : dict
            {chrom: (start, end)}
        snps_only : bool
            Filter out anything longer that 1 base

        Returns
        -------
        list
            A list of DB.Row objects

        Example
        -------
        >>> db.between({'chr6':  (116671802, 116671840),
                        'chr17': (71026530, 71026540)})
        [rs951565928<chr6:116671802-116671803>,
         rs756327030<chr6:116671836-116671837>,
         rs375317044<chr6:116671837-116671838>,
         rs148440225<chr17:71026531-71026532>,
         rs781305685<chr17:71026539-71026540>,
        """
        if not isinstance(locations, dict):
            raise ValueError('locations must be a dictionary, is {}'
                             .format(type(locations)))

        results = []
        for chrom in sorted(locations.keys(), key=chrom_key):
            start, end = locations[chrom]
            results += self.query().filter(
                self.Row.chrom == chrom
            ).filter(
                self.Row.start.between(start, end)
            ).all()

        return results

    def lookup_rsids(self, rsids):
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

    def lookup_location(self, chrom, start, end=None):
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

    def lookup_locations(self, locs):
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
        for chrom, starts in locs.items():
            for chunk in [starts[i:i + 990] for i in range(0, len(starts), 990)]:
                query =  self.query().filter(
                    self.Row.chrom == chrom
                ).filter(
                    self.Row.start.in_(chunk)
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


def chrom_key(chrom):
    """Use with sorted: return integer representation of chromosome."""
    if chrom.startswith('chr'):
        chrom = chrom[3:]
    if chrom.upper() == 'X':
        return 99
    elif chrom.upper() == 'Y':
        return 100
    elif chrom.upper().startswith('M'):
        return 101
    else:
        try:
            return int(chrom)
        except ValueError:
            return chrom

