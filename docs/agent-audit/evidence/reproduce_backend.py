"""Run the actual extracted trickle method with only its infrastructure replaced.

This is a focused control-flow reproduction, not the Django/JRTC test suite.
"""
import ast
import contextlib
import json
from pathlib import Path
from types import SimpleNamespace

source = Path(__file__).resolve().parents[3] / 'src/apps/meetings/services/signaling.py'
tree = ast.parse(source.read_text())
cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'MeetingMediaSignalService')
method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'trickle')
method.decorator_list = []
module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), method], type_ignores=[])
handle = SimpleNamespace(lifecycle_state='ready', selected_streams=[], save=lambda **kw: None)
claim = SimpleNamespace(command_applied=False)
calls = []
env = {
 'JanusHandleType': SimpleNamespace(PUBLISHER='publisher', SUBSCRIBER='subscriber'),
 '_get_or_create_media_handle': lambda **kw: handle,
 'ensure_participant_media_plugin': lambda *a, **kw: object(),
 '_serialize_trickle_candidates': lambda value: value,
 '_media_command_claim': lambda *a, **kw: contextlib.nullcontext(claim),
 'call_plugin_method': lambda obj, name, *a: calls.append((name, len(a[0]) if a else 0)),
 'transaction': SimpleNamespace(atomic=contextlib.nullcontext),
 '_lock_media_command_result': lambda claim: handle,
 'timezone': SimpleNamespace(now=lambda: None),
 '_serialize_handle_streams': lambda handle: [],
}
exec(compile(ast.fix_missing_locations(module), str(source), 'exec'), env)
for case, candidates, completed in [('final-three', [{}, {}, {}], True), ('empty-not-completed', [], False)]:
 calls.clear()
 env['trickle'](participant=SimpleNamespace(pk='p'), connection=None, handle_type='publisher', candidates=candidates, completed=completed)
 print(json.dumps({'case': case, 'calls': calls}))
