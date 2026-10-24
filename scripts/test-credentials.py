#!/usr/bin/env python3
"""Test credential input and migration without reading host credentials."""
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import pty
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


loader = importlib.machinery.SourceFileLoader('credentials', str(Path(__file__).resolve().parents[1] / 'packaging/debian/otelcol-extra-set-credentials'))
spec = importlib.util.spec_from_loader(loader.name, loader)
helper = importlib.util.module_from_spec(spec)
loader.exec_module(helper)


class InputTests(unittest.TestCase):
    def run_helper(self, inputs, encrypt_error=None):
        output = io.StringIO()
        with patch.object(sys, 'argv', ['set-credentials', 'switchbot']), \
                patch.object(helper.os, 'geteuid', return_value=0), \
                patch.object(sys.stdin, 'isatty', return_value=True), \
                patch.object(helper.getpass, 'getpass', side_effect=inputs), \
                patch.object(helper, 'encrypt', side_effect=encrypt_error) as encrypt, \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = helper.main()
        return result, output.getvalue(), encrypt

    def test_invalid_token_is_explained_and_retried_before_secret(self):
        result, output, encrypt = self.run_helper(['', 'pasted fixture with spaces', 'token-fixture', 'secret-fixture'])
        self.assertEqual(0, result)
        self.assertIn('SwitchBot Token: the value is empty', output)
        self.assertIn('spaces or line breaks are present', output)
        self.assertNotIn('TPM', output)
        self.assertNotIn('pasted fixture', output)
        self.assertNotIn('token-fixture', output)
        self.assertNotIn('secret-fixture', output)
        payload, component, key = encrypt.call_args.args
        self.assertEqual({'token': 'token-fixture', 'secret': 'secret-fixture'}, json.loads(payload))
        self.assertEqual(('switchbot', 'auto'), (component, key))

    def test_invalid_secret_exhaustion_never_changes_existing_credentials(self):
        result, output, encrypt = self.run_helper(['token-fixture', '秘密fixture', 'bad\x1bfixture', ''])
        self.assertEqual(1, result)
        self.assertIn('non-ASCII or control characters', output)
        self.assertIn('after three attempts', output)
        self.assertNotIn('TPM', output)
        self.assertNotIn('fixture', output)
        encrypt.assert_not_called()

    def test_terminal_and_encryption_failures_have_different_diagnostics(self):
        result, output, encrypt = self.run_helper([OSError('token-fixture')])
        self.assertEqual(1, result)
        self.assertIn('Failed during credential input', output)
        self.assertNotIn('token-fixture', output)
        self.assertNotIn('TPM', output)
        encrypt.assert_not_called()
        result, output, encrypt = self.run_helper(['token-fixture', 'secret-fixture'],
                                                subprocess.CalledProcessError(1, 'private-command', stderr=b'secret-fixture'))
        self.assertEqual(1, result)
        self.assertIn('credential encryption and registration', output)
        self.assertIn('systemd/TPM', output)
        self.assertNotIn('fixture', output)
        self.assertNotIn('private-command', output)

    def test_cancel_and_echo_fallback_never_encrypt(self):
        for error, code in [(EOFError(), 130), (KeyboardInterrupt(), 130), (helper.getpass.GetPassWarning(), 1)]:
            with self.subTest(error=type(error).__name__):
                result, output, encrypt = self.run_helper([error])
                self.assertEqual(code, result)
                self.assertNotIn('TPM', output)
                encrypt.assert_not_called()

    def test_stdin_errors_are_redacted_and_never_encrypt(self):
        for payload, message in [(b'fixture\n', 'wrong number'), (b'private fixture\nsecret-fixture\n', 'spaces'),
                                 (b'\xffprivate-fixture\nsecret-fixture\n', 'ASCII')]:
            with self.subTest(message=message):
                output = io.StringIO()
                with patch.object(sys, 'argv', ['set-credentials', 'switchbot', '--stdin']), \
                        patch.object(helper.os, 'geteuid', return_value=0), \
                        patch.object(sys, 'stdin', io.TextIOWrapper(io.BytesIO(payload), encoding='ascii')), \
                        patch.object(helper, 'encrypt') as encrypt, contextlib.redirect_stderr(output):
                    self.assertEqual(1, helper.main())
                self.assertIn(message, output.getvalue())
                self.assertNotIn('fixture', output.getvalue())
                self.assertNotIn('TPM', output.getvalue())
                encrypt.assert_not_called()

    def test_real_terminal_retry_keeps_token_and_secret_hidden(self):
        pid, terminal = pty.fork()
        if pid == 0:
            try:
                def encrypt(payload, component, key):
                    assert json.loads(payload) == {'token': 'pty-token-fixture', 'secret': 'pty-secret-fixture'}
                with patch.object(sys, 'argv', ['set-credentials', 'switchbot']), \
                        patch.object(helper.os, 'geteuid', return_value=0), patch.object(helper, 'encrypt', side_effect=encrypt):
                    result = helper.main()
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(result)
            except BaseException:
                os._exit(99)
        output = b''
        try:
            expected = [(b'SwitchBot Token (hidden): ', b'\n'),
                        (b'SwitchBot Token (hidden): ', b'pty-token-fixture\n'),
                        (b'SwitchBot Secret (hidden): ', b'pty-secret-fixture\n')]
            position = 0
            deadline = time.monotonic() + 5
            for prompt, value in expected:
                while prompt not in output[position:]:
                    self.assertLess(time.monotonic(), deadline, 'terminal prompt timed out')
                    if select.select([terminal], [], [], 0.1)[0]:
                        output += os.read(terminal, 8192)
                position = output.index(prompt, position) + len(prompt)
                os.write(terminal, value)
            while time.monotonic() < deadline:
                if select.select([terminal], [], [], 0.1)[0]:
                    try:
                        output += os.read(terminal, 8192)
                    except OSError:
                        break
            child, status = os.waitpid(pid, os.WNOHANG)
            while child == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
                child, status = os.waitpid(pid, os.WNOHANG)
            self.assertEqual(pid, child, 'terminal child did not exit')
            pid = 0
            self.assertEqual(0, os.waitstatus_to_exitcode(status))
            self.assertIn(b'the value is empty', output)
            self.assertIn(b'Encrypted credentials saved', output)
            self.assertNotIn(b'pty-token-fixture', output)
            self.assertNotIn(b'pty-secret-fixture', output)
        finally:
            os.close(terminal)
            if pid:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.patches = []
        for name in ['ETC', 'LEGACY', 'STORE', 'DROPINS', 'DEFAULT', 'OLD_STATE', 'NEW_STATE']:
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if name != 'DEFAULT':
                target.mkdir()
            patcher = patch.object(helper, name, target)
            patcher.start()
            self.patches.append(patcher)
        helper.DEFAULT.write_text('default configuration\n')
        (helper.ETC / 'config.yaml').write_bytes(helper.DEFAULT.read_bytes())
        (helper.LEGACY / 'config.yaml').write_text('token: ${env:DISCORD_BOT_TOKEN}\n# keep channels\n')

    def tearDown(self):
        for patcher in reversed(self.patches):
            patcher.stop()
        self.directory.cleanup()

    def test_encryption_failure_retains_old_data_and_new_default(self):
        legacy = helper.LEGACY / 'environment'
        legacy.write_text('DISCORD_BOT_TOKEN=fixture-token\nEXTRA_SETTING=keep\n')
        with patch.object(helper, 'encrypt', side_effect=ValueError('fixture failure')):
            with self.assertRaises(ValueError):
                helper.migrate('host')
        self.assertIn('fixture-token', legacy.read_text())
        self.assertEqual(helper.DEFAULT.read_bytes(), (helper.ETC / 'config.yaml').read_bytes())

    def test_encrypted_identity_preserved_and_new_config_takes_precedence(self):
        old = helper.STORE / 'discord-otelcol.token'
        old.write_bytes(b'cipher-fixture')
        with patch.object(helper, 'decrypt', return_value=b'fixture-token') as decrypt:
            helper.migrate('host')
            self.assertEqual(b'cipher-fixture', helper.credential_path('discord').read_bytes())
            self.assertTrue(all(call.args[1] == 'discord_bot_token' for call in decrypt.call_args_list))
            (helper.ETC / 'config.yaml').write_text('custom new configuration\n')
            helper.migrate('host')
            self.assertEqual('custom new configuration\n', (helper.ETC / 'config.yaml').read_text())

    def test_plaintext_removed_only_after_success_and_state_symlinks_not_followed(self):
        legacy = helper.LEGACY / 'environment'
        legacy.write_text('DISCORD_BOT_TOKEN=fixture-token\nEXTRA_SETTING=keep\n')
        secret_target = Path(self.directory.name) / 'private-fixture'
        secret_target.write_text('do not copy target')
        (helper.OLD_STATE / 'link').symlink_to(secret_target)
        def encrypt(payload, component, key):
            self.assertEqual(b'fixture-token', payload)
            helper.atomic_write(helper.credential_path(component), b'encrypted-fixture')
        with patch.object(helper, 'encrypt', side_effect=encrypt), patch.object(helper, 'decrypt', return_value=b'fixture-token'):
            helper.migrate('host')
        self.assertEqual('EXTRA_SETTING=keep\n', legacy.read_text())
        self.assertEqual('EXTRA_SETTING=keep\n', (helper.ETC / 'environment').read_text())
        self.assertTrue((helper.NEW_STATE / 'link').is_symlink())
        self.assertIn('${file:${env:CREDENTIALS_DIRECTORY}/discord_bot_token}', (helper.ETC / 'config.yaml').read_text())
        self.assertEqual(0o600, helper.credential_path('discord').stat().st_mode & 0o777)


if __name__ == '__main__':
    unittest.main()
