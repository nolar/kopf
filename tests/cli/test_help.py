

def test_help_in_root(invoke, mocker):
    result = invoke(['--help'])

    assert result.exit_code == 0
    assert 'Usage: kopf [OPTIONS]' in result.output
    assert '  run ' in result.output
    assert '  freeze ' in result.output
    assert '  resume ' in result.output


def test_help_in_subcommand(invoke, mocker):
    preload = mocker.patch('kopf.utilities.loaders.preload')
    real_run = mocker.patch('kopf.reactor.running.run')

    result = invoke(['run', '--help'])

    assert result.exit_code == 0
    assert not preload.called
    assert not real_run.called

    # Enough to be sure this is not a root command help.
    assert 'Usage: kopf run [OPTIONS]' in result.output
    assert '  --standalone' in result.output
    assert '  -m, --module' in result.output
