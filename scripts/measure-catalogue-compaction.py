#!/usr/bin/env python3
"""Measure the public compactor on an isolated 620-entry source."""
import json
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
subprocess.run(['cargo', 'build', '--locked', '--features', 'sqlite', '--example', 'catalogue_compaction'], cwd=root, check=True)
with tempfile.TemporaryDirectory(prefix='catalogue-compaction-') as directory:
    path = str(Path(directory) / 'source.db')
    binary = str(root / 'target/debug/examples/catalogue_compaction')
    subprocess.run([binary, path, 'seed'], check=True)
    def snapshot():
        with sqlite3.connect(path) as db:
            result = {'page_size': db.execute('pragma page_size').fetchone()[0],
                      'allocated_pages': db.execute('pragma page_count').fetchone()[0],
                      'reusable_pages': db.execute('pragma freelist_count').fetchone()[0]}
            for name in ['catalogue_source_entries', 'catalogue_retired_proof', 'catalogue_source_page', 'catalogue_compaction_candidates', 'sqlite_autoindex_catalogue_source_entries_1']:
                pages, size, cells = db.execute('select count(*),coalesce(sum(pgsize),0),coalesce(sum(ncell),0) from dbstat where name=?', (name,)).fetchone()
                result[name] = {'pages': pages, 'bytes': size, 'cells': cells}
            return result
    before = snapshot()
    start = time.perf_counter()
    moved = int(subprocess.check_output([binary, path, 'compact'], text=True))
    elapsed = (time.perf_counter() - start) * 1000
    assert moved == 159
    print(json.dumps({'before': before, 'after': snapshot(), 'moved': moved, 'process_ms': elapsed}, indent=2))
