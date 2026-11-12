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


def test_cli_routed_collect_and_dispatch(tmp_path):
    root=Path(__file__).parents[1];database=tmp_path/"cli-routed.sqlite3"
    routes=tmp_path/"routes.json"
    routes.write_text(json.dumps({"subscriptions":[
        {"recipient_id":"desk-eu","markets":["EU"],"min_severity":"high",
         "endpoint":"http://127.0.0.1/local/eu"},
        {"recipient_id":"desk-us","markets":["US"],"min_severity":"high",
         "endpoint":"http://127.0.0.1/local/us"}]}),encoding="utf-8")
    env={**os.environ,"PYTHONPATH":str(root/"src"),"PYTHONDONTWRITEBYTECODE":"1"}
    def run(*args):
        result=subprocess.run([sys.executable,"-m","recall_signal_hub.cli",*args,
                               "--database",str(database),"--routes",str(routes)],
                              cwd=root,env=env,text=True,capture_output=True,check=True)
        return json.loads(result.stdout)
    collected=run("collect","--fixture","fixtures/recalls.json")
    # Two fixture alerts: one EU case and one US case, each matching one desk.
    assert collected["counts"]["outbox_alerts"]==2
    assert collected["counts"]["outbox_targets"]==2
    assert collected["counts"]["outbox"]==0
    assert run("dispatch")["delivered"]==2
    assert run("dispatch")["delivered"]==0


def test_cli_rejects_invalid_routing_file(tmp_path):
    root=Path(__file__).parents[1];database=tmp_path/"cli-bad.sqlite3"
    routes=tmp_path/"bad.json"
    routes.write_text(json.dumps({"subscriptions":[
        {"recipient_id":"desk","markets":["EU"],"min_severity":"bogus",
         "endpoint":"http://127.0.0.1/local"}]}),encoding="utf-8")
    env={**os.environ,"PYTHONPATH":str(root/"src"),"PYTHONDONTWRITEBYTECODE":"1"}
    result=subprocess.run([sys.executable,"-m","recall_signal_hub.cli","collect",
                           "--fixture","fixtures/recalls.json","--database",str(database),
                           "--routes",str(routes)],cwd=root,env=env,text=True,capture_output=True)
    assert result.returncode==2 and "routing configuration" in result.stderr
