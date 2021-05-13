from kopf._core.intents.piggybacking import has_service_account, login_with_service_account

# As per https://kubernetes.io/docs/tasks/run-application/access-api-from-pod/
NAMESPACE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
SA_TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'
CA_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'


def test_has_no_serviceaccount_when_special_file_is_absent(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    result = has_service_account()
    assert result is False
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0] == SA_TOKEN_PATH


def test_has_serviceaccount_when_special_file_exists(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    result = has_service_account()
    assert result is True
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0] == SA_TOKEN_PATH


def test_serviceaccount_with_all_absent_files(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=False)  # all 3 of them.
    open_mock = mocker.patch('kopf._core.intents.piggybacking.open')
    open_mock.return_value.__enter__.return_value.read.return_value = ''
    credentials = login_with_service_account()
    assert credentials is None
    assert exists_mock.call_count == 1
    assert exists_mock.call_args_list[0][0][0] == SA_TOKEN_PATH
    assert not open_mock.called


def test_serviceaccount_with_all_present_files(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=True)  # all 3 of them.
    open_mock = mocker.patch('kopf._core.intents.piggybacking.open')
    open_mock.return_value.__enter__.return_value.read.side_effect=[' tkn ', ' ns ', RuntimeError]
    credentials = login_with_service_account()
    assert credentials is not None
    assert credentials.server == 'https://kubernetes.default.svc'
    assert credentials.default_namespace == 'ns'
    assert credentials.token == 'tkn'
    assert credentials.ca_path == CA_PATH
    assert exists_mock.call_count == 3
    assert exists_mock.call_args_list[0][0][0] == SA_TOKEN_PATH
    assert exists_mock.call_args_list[1][0][0] == NAMESPACE_PATH
    assert exists_mock.call_args_list[2][0][0] == CA_PATH
    assert open_mock.call_count == 2
    assert open_mock.call_args_list[0][0][0] == SA_TOKEN_PATH
    assert open_mock.call_args_list[1][0][0] == NAMESPACE_PATH
    # NB: the order is irrelevant and can be changed if needed.


def test_serviceaccount_with_only_the_token_file(mocker):
    # NB: the order is irrelevant and can be changed if needed.
    exists_mock = mocker.patch('os.path.exists', side_effect=[True, False, False])
    open_mock = mocker.patch('kopf._core.intents.piggybacking.open')
    open_mock.return_value.__enter__.return_value.read.side_effect=[' tkn ', RuntimeError]
    credentials = login_with_service_account()
    assert credentials is not None
    assert credentials.server == 'https://kubernetes.default.svc'
    assert credentials.default_namespace is None
    assert credentials.token == 'tkn'
    assert credentials.ca_path is None
    assert exists_mock.call_count == 3
    assert exists_mock.call_args_list[0][0][0] == SA_TOKEN_PATH
    assert exists_mock.call_args_list[1][0][0] == NAMESPACE_PATH
    assert exists_mock.call_args_list[2][0][0] == CA_PATH
    assert open_mock.call_count == 1
    assert open_mock.call_args_list[0][0][0] == SA_TOKEN_PATH
