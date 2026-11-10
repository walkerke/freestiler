"""Validate the exact retained snapped failures against independent Shapely oracles."""
import argparse
import json
from pathlib import Path
import shapely
from shapely.geometry import shape

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('candidate', type=Path)
args = parser.parse_args()
source = json.loads((Path(__file__).parent / 'original-39-snapped-failures.json').read_text())
candidate = json.loads(args.candidate.read_text())
def keyed(rows):
    result = {(tuple(r['tile']), r['id']): r for r in rows}
    assert len(result) == len(rows), 'duplicate tile/feature identity'
    return result
before, after = keyed(source), keyed(candidate)
assert len(before) == len(after) == 39
assert before.keys() == after.keys(), 'tile address or feature identity changed'
for key, old in before.items():
    a, b = shape(old['geometry']), shape(after[key]['geometry'])
    assert not a.is_valid, (key, 'historical failure unexpectedly valid')
    assert b.is_valid, (key, shapely.is_valid_reason(b))
    assert a.area == b.area, (key, 'signed fill area changed')
    assert shapely.make_valid(a).symmetric_difference(b).area == 0, (key, 'filled set changed')
print('39 baseline invalid -> 0 candidate invalid; all tile/feature IDs, areas, and filled sets retained')
