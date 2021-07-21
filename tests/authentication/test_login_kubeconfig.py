import os

import pytest
import yaml

from kopf._cogs.structs.credentials import LoginError
from kopf._core.intents.piggybacking import has_kubeconfig, login_with_kubeconfig

MINICONFIG = '''
    kind: Config
    current-context: ctx
    contexts:
      - name: ctx
        context:
          cluster: clstr
          user: usr
    clusters:
      - name: clstr
    users:
      - name: usr
'''


@pytest.mark.parametrize('envs', [{}, {'KUBECONFIG': ''}], ids=['absent', 'empty'])
def test_has_no_kubeconfig_when_nothing_is_provided(mocker, envs):
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    mocker.patch.dict(os.environ, envs, clear=True)
    result = has_kubeconfig()
    assert result is False
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0].endswith('/.kube/config')


@pytest.mark.parametrize('envs', [{'KUBECONFIG': 'x'}], ids=['set'])
def test_has_kubeconfig_when_envvar_is_set_but_no_homedir(mocker, envs):
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    mocker.patch.dict(os.environ, envs, clear=True)
    result = has_kubeconfig()
    assert result is True
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0].endswith('/.kube/config')


@pytest.mark.parametrize('envs', [{}, {'KUBECONFIG': ''}], ids=['absent', 'empty'])
def test_has_kubeconfig_when_homedir_exists_but_no_envvar(mocker, envs):
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    mocker.patch.dict(os.environ, envs, clear=True)
    result = has_kubeconfig()
    assert result is True
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0].endswith('/.kube/config')


@pytest.mark.parametrize('envs', [{}, {'KUBECONFIG': ''}], ids=['absent', 'empty'])
def test_homedir_is_used_if_it_exists(tmpdir, mocker, envs):
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    open_mock = mocker.patch('kopf._core.intents.piggybacking.open')
    open_mock.return_value.__enter__.return_value.read.return_value = MINICONFIG
    mocker.patch.dict(os.environ, envs, clear=True)
    credentials = login_with_kubeconfig()
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0].endswith('/.kube/config')
    assert open_mock.call_count == 1
    assert open_mock.call_args_list[0][0][0].endswith('/.kube/config')
    assert credentials is not None


@pytest.mark.parametrize('envs', [{}, {'KUBECONFIG': ''}], ids=['absent', 'empty'])
def test_homedir_is_ignored_if_it_is_absent(tmpdir, mocker, envs):
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    open_mock = mocker.patch('kopf._core.intents.piggybacking.open')
    open_mock.return_value.__enter__.return_value.read.return_value = ''
    mocker.patch.dict(os.environ, envs, clear=True)
    credentials = login_with_kubeconfig()
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0].endswith('/.kube/config')
    assert open_mock.call_count == 0
    assert credentials is None


def test_absent_kubeconfig_fails(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    with pytest.raises(IOError):
        login_with_kubeconfig()


def test_corrupted_kubeconfig_fails(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    kubeconfig.write("""!!acb!.-//:""")  # invalid yaml
    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    with pytest.raises(yaml.YAMLError):
        login_with_kubeconfig()


def test_empty_kubeconfig_fails(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    kubeconfig.write('')
    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    with pytest.raises(LoginError) as err:
        login_with_kubeconfig()
    assert "context is not set" in str(err.value)


def test_mini_kubeconfig_reading(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    kubeconfig.write(MINICONFIG)

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.server is None
    assert credentials.insecure is None
    assert credentials.scheme is None
    assert credentials.token is None
    assert credentials.certificate_path is None
    assert credentials.certificate_data is None
    assert credentials.private_key_path is None
    assert credentials.private_key_data is None
    assert credentials.ca_path is None
    assert credentials.ca_data is None
    assert credentials.password is None
    assert credentials.username is None
    assert credentials.default_namespace is None


def test_full_kubeconfig_reading(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    kubeconfig.write('''
        kind: Config
        current-context: ctx
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
              namespace: ns
          - name: def
        clusters:
          - name: clstr
            cluster:
              server: https://hostname:1234/
              certificate-authority-data: base64dataA
              certificate-authority: /pathA
              insecure-skip-tls-verify: true
          - name: hij
        users:
          - name: usr
            user:
              username: uname
              password: passw
              client-certificate-data: base64dataC
              client-certificate: /pathC
              client-key-data: base64dataK
              client-key: /pathK
              token: tkn
          - name: klm
    ''')

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.server == 'https://hostname:1234/'
    assert credentials.insecure == True
    assert credentials.scheme is None
    assert credentials.token == 'tkn'
    assert credentials.certificate_path == '/pathC'
    assert credentials.certificate_data == 'base64dataC'
    assert credentials.private_key_path == '/pathK'
    assert credentials.private_key_data == 'base64dataK'
    assert credentials.ca_path == '/pathA'
    assert credentials.ca_data == 'base64dataA'
    assert credentials.password == 'passw'
    assert credentials.username == 'uname'
    assert credentials.default_namespace == 'ns'


def test_kubeconfig_with_provider_token(tmpdir, mocker):
    kubeconfig = tmpdir.join('config')
    kubeconfig.write('''
        kind: Config
        current-context: ctx
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
        clusters:
          - name: clstr
        users:
          - name: usr
            user:
              auth-provider:
                config:
                  access-token: provtkn
    ''')

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=str(kubeconfig))
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.token == 'provtkn'


def test_merged_kubeconfigs_across_currentcontext(tmpdir, mocker):
    kubeconfig1 = tmpdir.join('config1')
    kubeconfig1.write('''
        kind: Config
        current-context: ctx
    ''')
    kubeconfig2 = tmpdir.join('config2')
    kubeconfig2.write('''
        kind: Config
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
              namespace: ns
        clusters:
          - name: clstr
            cluster:
              server: srv
        users:
          - name: usr
            user:
              token: tkn
    ''')

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=f'{kubeconfig1}{os.pathsep}{kubeconfig2}')
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.default_namespace == 'ns'
    assert credentials.server == 'srv'
    assert credentials.token == 'tkn'


def test_merged_kubeconfigs_across_contexts(tmpdir, mocker):
    kubeconfig1 = tmpdir.join('config1')
    kubeconfig1.write('''
        kind: Config
        current-context: ctx
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
              namespace: ns
    ''')
    kubeconfig2 = tmpdir.join('config2')
    kubeconfig2.write('''
        kind: Config
        clusters:
          - name: clstr
            cluster:
              server: srv
        users:
          - name: usr
            user:
              token: tkn
    ''')

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=f'{kubeconfig1}{os.pathsep}{kubeconfig2}')
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.default_namespace == 'ns'
    assert credentials.server == 'srv'
    assert credentials.token == 'tkn'


def test_merged_kubeconfigs_first_wins(tmpdir, mocker):
    kubeconfig1 = tmpdir.join('config1')
    kubeconfig1.write('''
        kind: Config
        current-context: ctx
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
              namespace: ns1
        clusters:
          - name: clstr
            cluster:
              server: srv1
        users:
          - name: usr
            user:
              token: tkn1
    ''')
    kubeconfig2 = tmpdir.join('config2')
    kubeconfig2.write('''
        kind: Config
        current-context: ctx
        contexts:
          - name: ctx
            context:
              cluster: clstr
              user: usr
              namespace: ns2
        clusters:
          - name: clstr
            cluster:
              server: srv2
        users:
          - name: usr
            user:
              token: tkn2
    ''')

    mocker.patch.dict(os.environ, clear=True, KUBECONFIG=f'{kubeconfig1}{os.pathsep}{kubeconfig2}')
    credentials = login_with_kubeconfig()

    assert credentials is not None
    assert credentials.default_namespace == 'ns1'
    assert credentials.server == 'srv1'
    assert credentials.token == 'tkn1'
