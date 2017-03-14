import app
import unittest

class AppTest(unittest.TestCase):

    def setUp(self):
        self.DB = app.InMemoryDB()

    def testGeneral(self):
        input = [{"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"},
                 {"type": "CUSTOMER", "verb": "NEW", "key": "96f55cghd8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Johnes", "adr_city": "Palo Alto", "adr_state": "CA"},
                 {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},
                 {"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1d9f", "event_time": "2017-01-06T12:47:12.344Z", "customer_id": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D"},
                 {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"},
                 {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a63", "event_time": "2017-02-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "25.03 USD"},
                 {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a431", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55cghd8f42", "total_amount": "2.34 USD"},
                 {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a632", "event_time": "2017-01-07T12:55:55.555Z", "customer_id": "96f55cghd8f42", "total_amount": "35.03 USD"}
        ]
        for e in input:
            app.Ingest(e, self.DB)
        actual_result = app.TopXSimpleLTVCustomers(10, self.DB)
        expected_result = ['Johnes', 'Smith']
        self.assertEqual(expected_result, actual_result)

    def testNoEvents(self):
        input = []
        for e in input:
            app.Ingest(e, self.DB)
        actual_result = app.TopXSimpleLTVCustomers(10, self.DB)
        expected_result = []
        self.assertEqual(expected_result, actual_result)


if __name__ == '__main__':
    unittest.main()
