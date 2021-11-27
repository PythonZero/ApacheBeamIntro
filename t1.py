import apache_beam as beam
from functools import partial
from apache_beam.dataframe import convert

from constants import TRANSACTION_AMOUNT_COL, TIMESTAMP_COL
from os_utils import clear_output
from timestamp_utils import filter_timestamps, convert_timestamp


def run_json():
    print("Running Pipeline -> Outputting to JSONL format")
    clear_output("jsonl")

    with beam.Pipeline() as pipeline:
        beam_df = pipeline | "Read CSV" >> beam.dataframe.io.read_csv(
            "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
        )
        (
            convert.to_pcollection(beam_df)
            | "Filter Trnx Amt" >> beam.Filter(lambda row: float(row[TRANSACTION_AMOUNT_COL]) > 20)
            | "Filter Date" >> beam.Filter(partial(filter_timestamps, year=2010))
            | "Convert date "
            >> beam.Map(
                lambda row: (convert_timestamp(row[TIMESTAMP_COL]).date().strftime("%Y-%m-%d"), row[1], row[2], row[3])
            )
            | "Group By timestamp"
            >> beam.GroupBy(lambda row: row[TIMESTAMP_COL]).aggregate_field(
                lambda row: row[TRANSACTION_AMOUNT_COL], sum, "total_quantity"
            )
            # | 'Print' >> beam.Map(print)  # for debugging
            | "JSONL format"
            >> beam.Map(
                lambda row: str({col_name: val for (col_name, val) in zip(["date", "transaction_amount"], row)})
            )
            | "Write to out" >> beam.io.WriteToText("output/results", file_name_suffix=".jsonl")
        )


def run_csv():
    print("Running Pipeline -> Outputting to CSV format")
    clear_output("csv")
    with beam.Pipeline() as pipeline:
        beam_df = pipeline | "Read CSV" >> beam.dataframe.io.read_csv(
            "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
        )

        (
            convert.to_pcollection(beam_df)
            | "Filter Trnx Amt" >> beam.Filter(lambda row: float(row[TRANSACTION_AMOUNT_COL]) > 20)
            | "Filter Date" >> beam.Filter(partial(filter_timestamps, year=2010))
            | "Convert date "
            >> beam.Map(
                lambda row: (convert_timestamp(row[TIMESTAMP_COL]).date().strftime("%Y-%m-%d"), row[1], row[2], row[3])
            )
            | "Group By timestamp"
            >> beam.GroupBy(lambda row: row[TIMESTAMP_COL]).aggregate_field(
                lambda row: row[TRANSACTION_AMOUNT_COL], sum, "total_quantity"
            )
            # | 'Print' >> beam.Map(print)  # for debugging
            | "CSV format" >> beam.Map(lambda row: ", ".join([f'"{column}"' for column in row]))
            | "Write to out"
            >> beam.io.WriteToText("output/results", file_name_suffix=".csv", header='"date", "transaction_amount"')
        )


if __name__ == "__main__":
    run_csv()
    run_json()
