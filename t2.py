import apache_beam as beam
from functools import partial

from apache_beam.dataframe import convert
from apache_beam.io.filesystem import CompressionTypes

from constants import TRANSACTION_AMOUNT_COL, TIMESTAMP_COL
from os_utils import clear_output
from timestamp_utils import filter_timestamps, convert_timestamp


class FilterTrnsxsGroupDate(beam.PTransform):
    def __init__(self, minimum_transaction_amount, minimum_year):
        """
        :param minimum_transaction_amount: The minimum transaction amount (to filter out)
        :param minimum_year: The minimum year (to filter out transactions)
        """
        self.minimum_transaction_amount = minimum_transaction_amount
        self.minimum_year = minimum_year
        super().__init__()

    def expand(self, p_collection):
        return (
            p_collection
            | "Filter Trnx Amt"
            >> beam.Filter(lambda row: float(row[TRANSACTION_AMOUNT_COL]) > self.minimum_transaction_amount)
            | "Filter Date" >> beam.Filter(partial(filter_timestamps, year=self.minimum_year))
            | "Convert date "
            >> beam.Map(
                lambda row: (convert_timestamp(row[TIMESTAMP_COL]).date().strftime("%Y-%m-%d"), row[1], row[2], row[3])
            )
            | "Group By timestamp"
            >> beam.GroupBy(lambda row: row[TIMESTAMP_COL]).aggregate_field(
                lambda row: row[TRANSACTION_AMOUNT_COL], sum, "total_quantity"
            )
        )


class OutputFormat(beam.PTransform):
    def __init__(self, save_as_csv=True, debug=False):
        """The output to format.
        If debug=True, outputs the code to stdout as print statements.
        Otherwise saves it either as csv OR jsonl format

        :param save_as_csv: If True -> saves as csv, otherwise saves as JSONL
        :param debug: If True, does not save the file, but outputs to print/stdout
        """
        self.save_as_csv = save_as_csv
        self.debug = debug
        super().__init__()

    def expand(self, p_collection):
        if self.debug:
            return p_collection | "Print" >> beam.Map(print)

        if self.save_as_csv:
            clear_output("csv.gz")
            return (
                p_collection
                | "CSV format" >> beam.Map(lambda row: ", ".join([f'{column}' for column in row]))
                | "Write to out"
                >> beam.io.WriteToText(
                    "output/results",
                    file_name_suffix=".csv.gz",
                    header='date, transaction_amount',
                    compression_type=CompressionTypes.GZIP,
                )
            )

        clear_output("jsonl.gz")
        return (
            p_collection
            | "JSONL format"
            >> beam.Map(
                lambda row: str({col_name: val for (col_name, val) in zip(["date", "transaction_amount"], row)})
            )
            | "Write to out"
            >> beam.io.WriteToText(
                "output/results", file_name_suffix=".jsonl.gz", compression_type=CompressionTypes.GZIP
            )
        )


def run(minimum_transaction_amount: int = 20, minimum_year: int = 2010, save_as_csv: bool = False, debug: bool = False):
    with beam.Pipeline() as pipeline:
        beam_df = pipeline | "Read CSV" >> beam.dataframe.io.read_csv(
            "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
        )
        p_collection = convert.to_pcollection(beam_df)
        out_collection = (
                p_collection
                | "Transform"
                >> FilterTrnsxsGroupDate(minimum_transaction_amount=minimum_transaction_amount, minimum_year=minimum_year)
                | "Output" >> OutputFormat(save_as_csv=save_as_csv, debug=debug)
        )
        return out_collection


if __name__ == "__main__":
    # CSV example
    run(minimum_transaction_amount=20, minimum_year=2010, save_as_csv=True, debug=False)

    # JSON example
    # run(minimum_transaction_amount=20, minimum_year=2010, save_as_csv=False, debug=False)
