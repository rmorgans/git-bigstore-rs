"""Regenerate the `stage_fields/` fixtures: .dvc files carrying the DVC 3
fields beyond `outs:` that real projects contain.

usage: python3 regen_stage_fields.py <dvc-binary> <outdir>
  e.g. python3 regen_stage_fields.py "$(command -v dvc)" .   # dvc==3.67.1

Isolated from ~/.config/dvc and ~/.dvc via DVC_*_CONFIG_DIR/DVC_SITE_CACHE_DIR.
Files DVC writes itself (`dvc add`, `dvc import-url`) are copied verbatim.
Hand-edited ones add only fields DVC 3's own schema allows
(dvc/schema.py SINGLE_STAGE_SCHEMA, dvc/output.py SCHEMA); every one is
checked by `dvc status` loading it, which fails on any field DVC rejects.
"""
import os, shutil, subprocess, sys, tempfile
from pathlib import Path

DVC, OUT = sys.argv[1], Path(sys.argv[2]).resolve() / 'stage_fields'
WORK = Path(tempfile.mkdtemp(prefix='bigstore-stage-fields-'))
os.environ.update(DVC_GLOBAL_CONFIG_DIR=str(WORK / '.g'), DVC_SYSTEM_CONFIG_DIR=str(WORK / '.s'),
                  DVC_SITE_CACHE_DIR=str(WORK / '.site'), DVC_NO_ANALYTICS='1')
shutil.rmtree(WORK, ignore_errors=True); shutil.rmtree(OUT, ignore_errors=True)
repo, src = WORK / 'repo', WORK / 'src'
(src / 'd').mkdir(parents=True); repo.mkdir(); OUT.mkdir()
(src / 'f.txt').write_text('hello\n'); (src / 'd/a').write_text('a'); (src / 'd/b').write_text('b')


def dvc(*args):
    subprocess.run([DVC, *args], cwd=repo, check=True)


def edit(name, top='', out=''):
    """Prepend top-level keys and append keys to the (single) output."""
    p = repo / name
    p.write_text(top + p.read_text() + out)


dvc('init', '--no-scm', '-q')

# dvc add of an executable file: DVC writes `isexec: true`.
(repo / 'run.sh').write_text('#!/bin/sh\n'); os.chmod(repo / 'run.sh', 0o755)
dvc('add', '-q', 'run.sh')

# dvc import-url (local path): stage `md5`, `frozen`, `deps`.
dvc('import-url', '-q', '../src/f.txt', 'imported.txt')
dvc('import-url', '-q', '../src/d', 'imported_dir')
# Not downloaded / not executed: the output has no md5.
dvc('import-url', '-q', '--no-download', '../src/f.txt', 'no_download.txt')
dvc('import-url', '-q', '--no-exec', '../src/f.txt', 'no_exec.txt')

# Annotations the DVC docs (user-guide/project-structure/dvc-files) allow,
# at the top level and on the output.
(repo / 'annotated').mkdir(); (repo / 'annotated/one.txt').write_text('1')
(repo / 'annotated/sub').mkdir(); (repo / 'annotated/sub/two.txt').write_text('2')
dvc('add', '-q', 'annotated')
edit('annotated.dvc',
     top='desc: Training images, v2\nmeta:\n  owner: rick\n  tags: [a, b]\n',
     out='  desc: the images\n  type: dataset\n  labels:\n  - train\n  meta:\n    source: camera\n'
         '  remote: other\n  push: false\n  persist: true\n')

# Pushed to a cloud-versioned remote: `cloud:` next to the md5.
(repo / 'versioned.bin').write_text('v')
dvc('add', '-q', 'versioned.bin')
edit('versioned.bin.dvc', out='  cloud:\n    myremote:\n      etag: 0x8DAC7C2F0E7A2F6\n'
                              '      version_id: 3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY\n')

# `cache: false`: DVC tracks the hash but keeps no copy.
(repo / 'uncached.bin').write_text('u')
dvc('add', '-q', 'uncached.bin')
edit('uncached.bin.dvc', out='  cache: false\n')

# A cloud file tracked by etag + version_id, with no md5.
(repo / 'etag_only.bin.dvc').write_text(
    'outs:\n- etag: 0x8DAC7C2F0E7A2F6\n  version_id: 3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY\n'
    '  size: 1\n  path: etag_only.bin\n')

# The output path is relative to `wdir`, not to the .dvc file.
(repo / 'sub').mkdir()
(repo / 'sub/wdir.dvc').write_text((repo / 'run.sh.dvc').read_text().replace('outs:', 'wdir: ..\nouts:'))

# Two outputs in one .dvc (legal in DVC's schema).
two = (repo / 'run.sh.dvc').read_text() + (repo / 'versioned.bin.dvc').read_text().split('outs:\n')[1]
(repo / 'two_outs.dvc').write_text(two.split('  cloud:')[0])

# DVC must load every file: `dvc status` fails on any schema violation.
dvc('status')
for f in sorted(p for p in repo.rglob('*.dvc') if p.is_file()):
    dest = OUT / f.relative_to(repo).as_posix().replace('/', '__')
    shutil.copy(f, dest)
    print(dest.name)
shutil.rmtree(WORK)
