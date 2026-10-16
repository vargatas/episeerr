"""Exact entrypoint/control flow tests with every media side effect stubbed."""
import ast
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def functions(filename, names):
    nodes = [node for node in ast.parse((ROOT / filename).read_text()).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    for node in nodes:node.decorator_list = []
    return compile(ast.Module(body=nodes, type_ignores=[]), filename, 'exec')


class EntryPointTest(unittest.TestCase):
    def namespace(self, argv, held=False):
        actions = []
        logger = Mock()
        ns = {'sys': types.SimpleNamespace(argv=['media_processor.py', *argv]),
              'os': os, 'time': time, 'json': json, 'logger': logger,
              'load_global_settings': lambda: {'automation_held': held},
              'run_unified_cleanup': lambda: actions.append('cleanup'),
              'get_series_id': lambda *args: 9,
              'load_config': lambda: {'rules': {'managed': {}}},
              'reconcile_series_drift': lambda *args: ('managed', False),
              'process_episodes_for_webhook': lambda *args, **kwargs: actions.append('webhook')}
        exec(functions('media_processor.py', {'main', 'get_server_activity'}), ns)
        return ns, actions, logger

    def test_cleanup_does_not_read_any_webhook_or_log_a_missing_event_error(self):
        for argv in ([], ['--cleanup']):
            ns, actions, logger = self.namespace(argv)
            ns['get_server_activity'] = Mock(side_effect=AssertionError('cleanup read event'))
            ns['main']()
            self.assertEqual(actions, ['cleanup'])
            logger.error.assert_not_called()

    def test_explicit_invalid_missing_directory_or_stale_event_never_falls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'event.json'
            cases = [(str(path), None), (directory, None), (str(path), '{invalid'),
                     (str(path), '{}'), (str(path), '[]')]
            for name, body in cases:
                if body is not None:path.write_text(body)
                for held in (False, True):
                    ns, actions, _ = self.namespace([name], held)
                    with self.assertRaises(SystemExit) as caught:ns['main']()
                    self.assertEqual(caught.exception.code, 1)
                    self.assertEqual(actions, [])
            path.write_text(json.dumps({'server_title': 'Fixture', 'server_season_num': 1, 'server_ep_num': 2}))
            os.utime(path, (time.time()-600, time.time()-600))
            ns, actions, _ = self.namespace([str(path)])
            with self.assertRaises(SystemExit):ns['main']()
            self.assertEqual(actions, [])

    def test_explicit_missing_file_cannot_substitute_legacy_payload(self):
        ns, actions, _ = self.namespace(['/missing-explicit-event'])
        real_exists = os.path.exists
        with patch.object(os.path, 'exists', side_effect=lambda path: True if str(path).startswith('/app/temp/') else real_exists(path)):
            with self.assertRaises(SystemExit):ns['main']()
        self.assertEqual(actions, [])

    def test_valid_explicit_event_processes_only_it_and_respects_hold(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'event.json'
            path.write_text(json.dumps({'server_title': 'Fixture', 'server_season_num': 1, 'server_ep_num': 2}))
            for held in (False, True):
                ns, actions, _ = self.namespace([str(path)], held)
                ns['main']()
                self.assertEqual(actions, [] if held else ['webhook'])

    def test_parallel_legacy_fallback_uses_private_payloads_and_cleans_them(self):
        with tempfile.TemporaryDirectory() as directory:
            barrier = threading.Barrier(2)
            observed = []
            def run(argv, **kwargs):
                self.assertEqual(len(argv), 3)
                path = Path(argv[2])
                data = json.loads(path.read_text())
                barrier.wait(timeout=10)
                self.assertTrue(path.exists())
                observed.append((str(path), data['server_title']))
                return types.SimpleNamespace(returncode=0, stderr='')
            tautulli = types.ModuleType('integrations.tautulli')
            tautulli.process_watch_event = Mock(side_effect=RuntimeError('injected integration failure'))
            processor = types.ModuleType('media_processor');processor.get_series_id = lambda *args: 9
            episeerr = types.ModuleType('episeerr');episeerr.load_config = lambda: {};episeerr.save_config = Mock()
            modules = {'integrations.tautulli': tautulli, 'media_processor': processor, 'episeerr': episeerr}
            def invoke(title):
                data = {'plex_title': title, 'plex_season_num': 1, 'plex_ep_num': 2}
                ns = {'request': types.SimpleNamespace(get_json=lambda **kwargs: data, json=data),
                      'current_app': types.SimpleNamespace(logger=Mock()), 'jsonify': lambda value: value,
                      'os': types.SimpleNamespace(path=os.path, makedirs=os.makedirs, getcwd=lambda: directory,
                                                  urandom=os.urandom, remove=os.remove),
                      'json': json, 'subprocess': types.SimpleNamespace(run=run),
                      'episeerr_utils': types.SimpleNamespace(reconcile_series_drift=lambda *args: ('managed', False))}
                exec(functions('webhooks.py', {'handle_server_webhook'}), ns)
                return ns['handle_server_webhook']()
            with patch.dict(sys.modules, modules), ThreadPoolExecutor(max_workers=2) as pool:
                self.assertEqual(list(pool.map(invoke, ('First', 'Second'))), [({'status': 'success'}, 200)] * 2)
            self.assertEqual({title for _, title in observed}, {'First', 'Second'})
            self.assertEqual(len({path for path, _ in observed}), 2)
            self.assertTrue(all(not Path(path).exists() for path, _ in observed))


if __name__ == '__main__':
    unittest.main()
