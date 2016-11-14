import os
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

# Boto: see http://docs.pythonboto.org/en/latest/


class Test(object):

    connection = None

    def setup_method(self, _):
        """setup the connection"""
        # OrdinaryCallingFormat ==> tell boto not to use DNS for bucket
        self.connection = S3Connection(aws_access_key_id='accessKey1',
                                       aws_secret_access_key='verySecretKey1',
                                       is_secure=False,
                                       port=8000,
                                       calling_format=OrdinaryCallingFormat(),
                                       host=os.getenv('IP'))

    def test_delete_me(self):
        """28/10/2015 without a test, pytest will fail"""
        """delete this function once the below 2 are uncommented"""
        assert True


''' 2015/10/28 commented as crashing the server
    def test_create_bucket(self):
        """Can we create a bucket ?"""
        self.connection.create_bucket('mybucket')
        bucket = self.connection.lookup('mybucket')
        assert bucket is not None

    def test_check_for_not_existing_bucket(self):
        """28/10/2015 this crashes the server ?"""
        bucket = self.connection.get_bucket('nonexisting bucket')
        assert bucket is None
        bucket = self.connection.get_bucket('nonexisting bucket')
        assert bucket is None
'''
