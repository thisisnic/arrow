# https://github.com/apache/arrow/issues/38427
# Script posted on the thread by a commenter (pyarrow 14.0.1, Python 3.11.6,
# macOS), unchanged. Added: version print, file count and RESULT line at the
# end. The report is that write_dataset hangs just before max_open_files is
# reached, so a driver timeout is the "reproduces" outcome.
import os
import platform
import random
import shutil
import string
from pathlib import Path
import pyarrow
import pyarrow.dataset as ds

def gen_batches(my_schema):
    batch_dict = create_empty_dict()
    for i in range(1000):
        batch_dict['part'].append(i)

        chars = ''.join(random.choice(string.ascii_letters) for _ in range(3))
        batch_dict['chars'].append(chars)

        if len(batch_dict['part']) >= 3:
            yield pyarrow.RecordBatch.from_pydict(batch_dict, my_schema)
            batch_dict = create_empty_dict()

def create_empty_dict():
    return {
        'part': [],
        'chars': []
    }

if __name__ == '__main__':
    print("pyarrow", pyarrow.__version__, "|", platform.platform(), flush=True)
    shutil.rmtree('./tmp/', ignore_errors=True)
    Path('./tmp/').mkdir(exist_ok=True)

    my_schema = pyarrow.schema([
        ('part', pyarrow.uint32()),
        ('chars', pyarrow.string())
    ])

    writer_args = {
        'schema': my_schema,
        'format': 'parquet',
        'partitioning': [
            'part'
        ],
        'partitioning_flavor': 'hive',
        'max_partitions': 1_000,
        'max_open_files': 20,
        'existing_data_behavior': 'delete_matching'
    }

    ds.write_dataset(
        gen_batches(my_schema),
        base_dir='./tmp/',
        **writer_args
    )

    n_files = sum(len(files) for _, _, files in os.walk('./tmp'))
    print(f"RESULT: write_dataset completed, {n_files} files written")
