"""Independent decoded baseline/candidate regression and timing receipt."""
import gzip, hashlib, json, statistics, subprocess, sys
from pathlib import Path
import mapbox_vector_tile
from pmtiles.reader import MmapSource, Reader, all_tiles
import shapely
from shapely.geometry import shape

import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-exe', required=True, type=Path)
parser.add_argument('--candidate-exe', required=True, type=Path)
parser.add_argument('--fixtures', required=True, type=Path, help='Directory containing cells7.geojson and regions7.geojson')
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
ROOT = args.output.resolve()
ROOT.mkdir(parents=True, exist_ok=True)
FIXTURES = args.fixtures.resolve()
BIN = {'baseline': args.baseline_exe.resolve(), 'candidate': args.candidate_exe.resolve()}

def decode(path,fmt):
    if fmt=='mlt':
        # The unmodified MLT encoder's pre-existing delta/zigzag ID mismatch is
        # reported separately. Exact source cell_id property is the comparison
        # key here; baseline/candidate decoded IDs must still match exactly.
        out={}
        for t in json.loads(Path(str(path)+'.decoded.json').read_text()):
            rows=t['collection']['features'];keyed={f['properties']['corpus_id']:f for f in rows}
            assert len(keyed)==len(rows),'duplicate corpus identities'
            out[tuple(t['address'])]=keyed
        return out
    out={}
    with path.open('rb') as stream:
        reader=Reader(MmapSource(stream))
        for address,raw in all_tiles(reader.get_bytes):
            decoded=mapbox_vector_tile.decode(gzip.decompress(raw),default_options={'y_coord_down':True})
            assert gzip.decompress(reader.get(*address))==gzip.decompress(raw)
            out[address]={f['id']:f for f in decoded['features']['features']}
            assert len(out[address])==len(decoded['features']['features']),'duplicate encoded feature IDs'
    return out

results=[]
for corpus,zoom in [('cells7',6),('regions7',12)]:
    source=json.loads((FIXTURES/f'{corpus}.geojson').read_text())['features']
    expected={int(f['id']):f['properties'] for f in source}
    assert len(expected)==len(source)
    assert len({(p['mesh_name'],p['cell_id']) for p in expected.values()})==len(source)
    for simplify in [False,True]:
        for fmt in ['mvt','mlt']:
            outputs={};timings={}
            for mode,exe in BIN.items():
                samples=[]
                for sample in range(3 if fmt=='mvt' and not simplify else 1):
                    path=ROOT/f'{corpus}-{mode}-{fmt}-s{int(simplify)}-{sample}.pmtiles'
                    run=subprocess.run([str(exe),str(FIXTURES/f'{corpus}.geojson'),str(path),str(zoom),str(simplify).lower(),fmt],capture_output=True,text=True,check=True)
                    samples.append(json.loads(run.stdout)['seconds'])
                    if sample==0: outputs[mode]=decode(path,fmt)
                timings[mode]=samples
            before,after=outputs['baseline'],outputs['candidate']
            assert before.keys()==after.keys()
            invalid={k:[] for k in outputs};changed=[];holes_before=holes_after=0;source_property_mismatches=[];source_id_mismatches=[]
            for address in before:
                assert before[address].keys()==after[address].keys()
                for fid,old in before[address].items():
                    new=after[address][fid]
                    assert old['properties']==new['properties']
                    assert old['id']==new['id']
                    for field,value in expected[fid].items():
                        if new['properties'][field]!=value:
                            assert fmt=='mlt' and isinstance(value,float),(address,fid,field)
                            source_property_mismatches.append(dict(tile=address,id=fid,field=field,source=value,decoded=new['properties'][field]))
                    if new['id']!=fid:source_id_mismatches.append(dict(tile=address,id=fid,decoded=new['id']))
                    if fmt=='mvt':assert new['id']==fid
                    a,b=shape(old['geometry']),shape(new['geometry'])
                    for mode,g in [('baseline',a),('candidate',b)]:
                        if not g.is_valid:invalid[mode].append({'tile':address,'id':fid,'reason':shapely.is_valid_reason(g)})
                    def holes(g):return sum(len(p.interiors) for p in (g.geoms if g.geom_type=='MultiPolygon' else [g]))
                    holes_before+=holes(a);holes_after+=holes(b)
                    if not a.equals_exact(b,0,normalize=True):
                        # MakeValid is an INDEPENDENT oracle here, never production repair.
                        oracle=shapely.make_valid(a)
                        assert oracle.symmetric_difference(b).area==0,(address,fid)
                        assert a.area==b.area,(address,fid,'signed fill changed')
                        changed.append({'tile':address,'id':fid,'before_type':a.geom_type,'after_type':b.geom_type})
            assert {fid for tile in after.values() for fid in tile}==set(expected)
            assert not invalid['candidate'],invalid['candidate']
            # A pinched exterior can encode an enclosed lobe as a repeated
            # boundary walk. Splitting exposes that same unfilled area as an
            # explicit interior ring; exact filled-set equality above guards it.
            results.append(dict(corpus=corpus,zoom=zoom,simplification=simplify,format=fmt,source_features=len(expected),tiles=len(after),tile_features=sum(map(len,after.values())),invalid=invalid,changed=changed,holes_before=holes_before,holes_after=holes_after,source_property_mismatches=source_property_mismatches,source_id_mismatches=source_id_mismatches,seconds=timings,median_seconds={k:statistics.median(v) for k,v in timings.items()}))
            print(corpus,fmt,simplify,'invalid',len(invalid['baseline']),'->',len(invalid['candidate']),'changed',len(changed),flush=True)
            (ROOT/'qualification.json').write_text(json.dumps({'binaries':{k:{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for k,p in BIN.items()},'results':results},indent=2),encoding='utf-8')
