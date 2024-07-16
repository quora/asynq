import json
import subprocess
from typing import Any


def _run_pyright(path) -> list[dict[str, Any]]:
    out = subprocess.check_output(["pyright", "--outputjson", path], text=True)
    diagnostics = json.loads(out).get("generalDiagnostics", [])
    return [{"message": d["message"], "rule": d["rule"]} for d in diagnostics]


def test_return_type():
    assert _run_pyright("typing_example/param_spec.py") == [
        dict(rule="", message=""),
        dict(rule="", message=""),
    ]
    assert _run_pyright("typing_example/return_type.py") == [
        dict(rule="", message=""),
        dict(rule="", message=""),
    ]
