import unittest

from aws_credentials import boto3_credentials, redshift_copy_credentials


class AwsSessionCredentialsTests(unittest.TestCase):
    def test_long_term_credentials_do_not_include_a_session_token(self):
        credentials = boto3_credentials({"access_key": "AKIA-test", "secret_key": "secret"})
        self.assertEqual(credentials, {
            "aws_access_key_id": "AKIA-test",
            "aws_secret_access_key": "secret",
        })
        self.assertNotIn("SESSION_TOKEN", redshift_copy_credentials({
            "access_key": "AKIA-test", "secret_key": "secret",
        }))

    def test_temporary_credentials_include_the_session_token(self):
        config = {"access_key": "ASIA-test", "secret_key": "secret", "session_token": "token"}
        self.assertEqual(boto3_credentials(config)["aws_session_token"], "token")
        self.assertIn("SESSION_TOKEN 'token'", redshift_copy_credentials(config))

