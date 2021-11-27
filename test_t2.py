from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam

from t2 import FilterTrnsxsGroupDate


def test_FilterTrnsxsGroupDate():
    input_data = [
        ["2015-01-01 00:00:00 UTC", "wallet1", "wallet2", 200],  # included (Group 1)
        ["2005-01-01 00:00:00 UTC", "wallet3", "wallet4", 100],  # excluded as (year 2005 < 2010)
        ["2020-01-01 00:00:00 UTC", "wallet5", "wallet6", 100],  # excluded as amnt 100 (must be greater than)
        ["2015-01-01 00:00:00 UTC", "wallet7", "wallet8", 300],  # included (Group 1)
        ["2020-01-01 00:00:00 UTC", "wallet9", "wallet10", 400],  # included (Group 2)

    ]
    with TestPipeline() as p:
        input_p_collection = p | beam.Create(input_data)
        output = input_p_collection | FilterTrnsxsGroupDate(minimum_transaction_amount=100, minimum_year=2010)
        assert_that(output, equal_to([("2015-01-01", 500),("2020-01-01", 400)]))
