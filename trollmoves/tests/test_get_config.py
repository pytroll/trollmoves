"""Unittesting the reading and extraction of clean configurations."""


from configparser import RawConfigParser


def test_read_config_params(minimal_config_file):
    """Test reading configuration from a config file."""
    conf = RawConfigParser()
    conf.read(minimal_config_file)

    info = dict(conf.items("DEFAULT"))
    assert info == {"mailhost": "localhost", "to": "some_users@xxx.yy", "subject": "Cleanup Error on {hostname}"}

    info = dict(conf.items("mytest_files1"))
    assert info == {'mailhost': 'localhost', 'to': 'some_users@xxx.yy', 'subject': 'Cleanup Error on {hostname}', 'base_dir': '/san1', 'templates': 'polar_in/sentinel3/olci/lvl1/*/*,polar_in/sentinel3/olci/lvl1/*', 'hours': '3'}  # noqa
