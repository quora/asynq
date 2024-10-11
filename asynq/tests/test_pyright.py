import json
import subprocess
from typing import Any, Dict, List


def _run_pyright(path) -> List[Dict[str, Any]]:
    p = subprocess.run(
        ["pyright", "--outputjson", path], text=True, capture_output=True
    )
    diagnostics = json.loads(p.stdout.replace("\xa0", " ")).get(
        "generalDiagnostics", []
    )
    return [
        {"message": d["message"], "rule": d["rule"], "range": d["range"]}
        for d in diagnostics
    ]


def test_return_type():
    assert _run_pyright("asynq/tests/typing_example/param_spec.py") == [
        dict(
            rule="reportArgumentType",
            message='Argument of type "Literal[\'1\']" cannot be assigned to parameter "arg1" of type "int" in function "asyncio"\n  "Literal[\'1\']" is not assignable to "int"',
            range={
                "start": {"line": 14, "character": 25},
                "end": {"line": 14, "character": 28},
            },
        ),
        dict(
            rule="reportArgumentType",
            message='Argument of type "Literal[2]" cannot be assigned to parameter "arg2" of type "str" in function "asyncio"\n  "Literal[2]" is not assignable to "str"',
            range={
                "start": {"line": 14, "character": 35},
                "end": {"line": 14, "character": 36},
            },
        ),
    ]
    assert _run_pyright("asynq/tests/typing_example/return_type.py") == [
        dict(
            rule="reportAssignmentType",
            message='Type "int" is not assignable to declared type "str"\n  "int" is not assignable to "str"',
            range={
                "start": {"line": 38, "character": 13},
                "end": {"line": 38, "character": 31},
            },
        ),
        dict(
            rule="reportAssignmentType",
            message='Type "int" is not assignable to declared type "str"\n  "int" is not assignable to "str"',
            range={
                "start": {"line": 41, "character": 13},
                "end": {"line": 41, "character": 31},
            },
        ),
        dict(
            rule="reportAssignmentType",
            message='Type "int" is not assignable to declared type "str"\n  "int" is not assignable to "str"',
            range={
                "start": {"line": 44, "character": 13},
                "end": {"line": 44, "character": 31},
            },
        ),
        dict(
            rule="reportAssignmentType",
            message='Type "int" is not assignable to declared type "str"\n  "int" is not assignable to "str"',
            range={
                "start": {"line": 47, "character": 13},
                "end": {"line": 47, "character": 31},
            },
        ),
    ]
