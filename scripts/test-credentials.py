#!/usr/bin/env python3
"""Test migration rollback and precedence without reading host credentials."""
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


loader = importlib.machinery.SourceFileLoader('credentials', str(Path(__file__).resolve().parents[1] / 'packaging/debian/otelcol-extra-set-credentials'))
spec = importlib.util.spec_from_loader(loader.name, loader)
helper = importlib.util.module_from_spec(spec)
loader.exec_module(helper)


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
