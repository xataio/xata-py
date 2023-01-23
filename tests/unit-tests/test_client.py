import unittest
import pytest
import os

from xata.client import XataClient

class TestClient(unittest.TestCase):



    """
     'apiKey': self.api_key,
     'location': self.api_key_location,
           'workspaceId': self.workspace_id,
           'region': self.region,
           'dbName': self.db_name,
           'branchName': self.branch_name,
    """

    def test_init_api_key_with_params(self):
        api_key = 'param_ABCDEF123456789'

        client = XataClient(api_key=api_key)
        cfg = client.get_config()

        assert 'apiKey' in cfg
        assert api_key == cfg['apiKey']
        assert 'apiKeyLocation' in cfg
        assert 'parameter' == cfg['apiKeyLocation']

    def test_init_api_key_via_envvar(self):
        api_key = 'envvar_ABCDEF123456789'
        os.environ['XATA_API_KEY'] = api_key

        client = XataClient()
        cfg = client.get_config()

        assert 'apiKey' in cfg
        assert api_key == cfg['apiKey']
        assert 'apiKeyLocation' in cfg
        assert 'env' == cfg['apiKeyLocation']

    def test_init_api_key_via_xatarc(self):
        api_key = 'xatarc_ABCDEF123456789'
        # TODO
        assert True == True

    def test_init_db_url(self):
        db_url = 'https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042'
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert 'workspaceId' in cfg
        assert 'py-sdk-unit-test-12345' == cfg['workspaceId']
        assert 'region' in cfg
        assert 'eu-west-1' == cfg['region']
        assert 'dbName' in cfg
        assert 'testopia-042' == cfg['dbName']

    def test_init_db_url_invalid_combinations(self):
        with pytest.raises(Exception):
            c = XataClient(db_url='db_url', workspace_id='ws_id')
        
        with pytest.raises(Exception):
            c = XataClient(db_url='db_url', db_name='db_name')
        
        with pytest.raises(Exception):
            c = XataClient(db_url='db_url', workspace_id='ws_id', db_name='db_name')
    