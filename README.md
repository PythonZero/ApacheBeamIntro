# Apache Beam Starter Exercise

# Quickstart

1. Install requirements using `pip install -r requirements.txt`
2. Run `t1.py` to use the non-composite transform approach
3. Run `t2.py` for the composite transform approach
   1. Change the parameters 
      1. `minimum_transaction_amount` 
      2. `minimum_year`
   2. Change the output by modifying
      1. `save_as_csv` - if True -> csv, if False -> JSON
      2. `debug` - if True -> disables saving & prints to stdout
4. Run `pytest` or `python -m pytest .` to run the tests
