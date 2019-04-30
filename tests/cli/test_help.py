

def test_help_in_root(invoke, mocker):
    login = mocker.patch('kopf.cli.login')

    result = invoke(['--help'])

    assert result.exit_code == 0
    assert not login.called

    assert 'Usage: kopf [OPTIONS]' in result.output
    assert '  run ' in result.output
    assert '  freeze ' in result.output
    assert '  resume ' in result.output


def test_help_in_subcommand(invoke, mocker):
    login = mocker.patch('kopf.cli.login')
    preload = mocker.patch('kopf.cli.preload')
    real_run = mocker.patch('kopf.cli.real_run')

    result = invoke(['run', '--help'])

    assert result.exit_code == 0
    assert not login.called
    assert not preload.called
    assert not real_run.called

    # Enough to be sure this is not a root command help.
    assert 'Usage: kopf run [OPTIONS]' in result.output
    assert '  --standalone' in result.output
    assert '  -m, --module' in result.output
