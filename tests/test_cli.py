import json
import os
from pathlib import Path
import subprocess
import sys


def test_cli_collect_inspect_dispatch(tmp_path):
    root=Path(__file__).parents[1];database=tmp_path/"cli.sqlite3"
    env={**os.environ,"PYTHONPATH":str(root/"src"),"PYTHONDONTWRITEBYTECODE":"1"}
    def run(*args):
        result=subprocess.run([sys.executable,"-m","recall_signal_hub.cli",*args,"--database",str(database)],
                              cwd=root,env=env,text=True,capture_output=True,check=True)
        return json.loads(result.stdout)
    assert run("collect","--fixture","fixtures/recalls.json")["counts"]["cases"]==2
    assert len(run("inspect")["cases"])==2
    assert run("dispatch")["delivered"]==2
    assert run("dispatch")["delivered"]==0
