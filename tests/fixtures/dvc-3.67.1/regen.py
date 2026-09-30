"""Regenerate DVC 3 golden fixtures with an isolated DVC install.

usage: python3 regen.py <dvc-binary> <outdir>
Isolates DVC from ~/.config/dvc (global cache dir/type) via DVC_*_CONFIG_DIR.
Each case dir gets: <out>.dvc, manifest.dir (raw .dir bytes), manifest.dir.id, tree.tar.
"""
import glob, os, shutil, subprocess, sys, unicodedata
from pathlib import Path

DVC, OUT = sys.argv[1], Path(sys.argv[2]).resolve()
OUT.mkdir(parents=True, exist_ok=True)
os.environ.update(DVC_GLOBAL_CONFIG_DIR=str(OUT / '.g'), DVC_SYSTEM_CONFIG_DIR=str(OUT / '.s'),
                  DVC_SITE_CACHE_DIR=str(OUT / '.site'), DVC_NO_ANALYTICS='1')


def dataset(d):
    (d / 'annotations').mkdir(parents=True); (d / 'views/v1').mkdir(parents=True); (d / 'views/empty_dir').mkdir()
    (d / 'annotations/labels.jsonl').write_text('{"a":1}\n{"a":2}\n'); (d / 'annotations/empty.parquet').write_bytes(b'')
    (d / 'views/v1/mask.json').write_text('{"m":[1,2]}'); (d / 'config.toml').write_text('x = 1\n')
    (d / 'Ünïcödé naïve.json').write_text('{}'); (d / 'B_upper.txt').write_text('B'); (d / 'a_lower.txt').write_text('a')
    (d / 'a-dash.txt').write_text('dash'); (d / 'a/b').mkdir(parents=True); (d / 'a/b/c.txt').write_text('c')
    (d / 'a.txt').write_text('adot'); os.symlink('config.toml', d / 'link_to_config')


def names(d):
    d.mkdir()
    for i, n in enumerate(['q"uote.txt', 'back\\slash.txt', 'tab\there.txt', 'new\nline.txt', 'del\x7f.txt',
                           'emoji\U0001F600.txt', unicodedata.normalize('NFD', 'café.txt'), '.DS_Store', 'Z.txt',
                           '~tilde.txt', '\u00a0nbsp.txt']):
        (d / n).write_text(str(i))
    (d / '.git').mkdir(); (d / '.git/HEAD').write_text('ref'); (d / 'sub').mkdir(); (d / 'sub/.hidden').write_text('h')


def dirsymlink(d):
    (d / 'real').mkdir(parents=True); (d / 'real/f.txt').write_text('f'); os.symlink('real', d / 'dirlink')


def empty(d):
    (d / 'sub').mkdir(parents=True)


def nested_scm(d):
    (d / 'inner/.git').mkdir(parents=True); (d / 'inner/a.txt').write_text('a'); (d / 'top.txt').write_text('t')
    (d / 'dv/.dvc').mkdir(parents=True); (d / 'dv/b.txt').write_text('b')


def crlf(d):
    d.mkdir(); (d / 'crlf.txt').write_bytes(b'a\r\nb\r\n')


for case, out, build in [('dataset', 'tt', dataset), ('names', 'x', names), ('dirsymlink', 's', dirsymlink),
                         ('empty', 'e', empty), ('nested_scm', 'n', nested_scm), ('crlf', 'c', crlf)]:
    work = OUT / '.work' / case
    shutil.rmtree(work, ignore_errors=True); work.mkdir(parents=True)
    subprocess.run([DVC, 'init', '--no-scm', '-q'], cwd=work, check=True)
    build(work / out)
    subprocess.run([DVC, 'add', '-q', out], cwd=work, check=True)
    g = OUT / case; shutil.rmtree(g, ignore_errors=True); g.mkdir()
    shutil.copy(work / f'{out}.dvc', g)
    [manifest] = glob.glob(str(work / '.dvc/cache/files/md5/*/*.dir'))
    shutil.copy(manifest, g / 'manifest.dir')
    (g / 'manifest.dir.id').write_text(Path(manifest).parent.name + Path(manifest).name + '\n')
    subprocess.run(['tar', '-cf', str(g / 'tree.tar'), '-C', str(work), out], check=True)
    if case == 'dataset':
        subprocess.run([DVC, 'remote', 'add', '-d', 'r', str(g / 'remote_dvc_push')], cwd=work, check=True)
        subprocess.run([DVC, 'push', '-q'], cwd=work, check=True)
    print(case, (g / 'manifest.dir.id').read_text().strip())
