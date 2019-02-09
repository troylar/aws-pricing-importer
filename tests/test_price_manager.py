import unittest
from pricing import PriceManager


class TestPriceManager(unittest.TestCase):

    def test_kwarg_prefix(self):
        pm = PriceManager(Prefix='test')
        assert pm.prefix == 'test'

    def test_kwarg_price_folder(self):
        pm = PriceManager(PriceFolder='test')
        assert pm.price_folder == 'test'

    def test_kwarg_ddl_folder(self):
        pm = PriceManager(DDLFolder='test')
        assert pm.ddl_folder == 'test'

    def test_kwarg_athena_database(self):
        pm = PriceManager(AthenaDatabase='test')
        assert pm.athena_database == 'test'

    def test_kwarg_price_url(self):
        pm = PriceManager(PriceUrl='test')
        assert pm.price_url == 'test'

    def test_kwarg_bucket_name(self):
        pm = PriceManager(BucketName='test')
        assert pm.bucket_name == 'test'
