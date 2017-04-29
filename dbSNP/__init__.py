# -*- coding: utf-8 -*-
"""
SQLAlchemy Interface to dbSNP Database, particularly an SQLITE version.

Info
----
Author: Michael D Dacre, mike.dacre@gmail.com
Organization: Stanford University
License: MIT License, property of Stanford, use as you wish
Version: 0.1.0a

Examples
--------
Connect to the database
>>> db = dnSNP.DB('/path/to/database/', version=150)

Lookup rsIDs
>>> db.lookup_rsids(['rs564732507', 'rs1050043376'])
[rs1050043376<chr8:4330858-4330859>, rs564732507<chr7:1052302-1052303>]

Lookup locations
>>> db.lookup_location
rs1050043376<chr8:4330858-4330859>
>>> db.lookup_locations({'chrX': [114051427, 95976324], 'chr7': [55817861]})
[rs747708367<chrX:95976324-95976325>,
 rs145320604<chrX:114051427-114051428>,
 rs931602891<chr7:55817861-55817862>]

Select SNPs randomly
>>> import random
>>> random.seed(1)
>>> db.random()
[rs760603628<chr4:150489434-150489435>]
>>> db.random(3)
[rs548062961<chr8:7209397-7209398>,
 rs370141878<chr9:35476705-35476706>,
 rs777569312<chrX:98915204-98915205>]
"""
__version__ = '0.1.0a'

# Make core functionality available from the top level
from .db import DB
from .build import build_db
