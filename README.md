# AWS Pricing Importer

## Overview
The purpose of this project is to import *all* of the pricing data points of AWS into [Amazon Athena](https://aws.amazon.com/athena/) to easily query via [Presto SQL](https://prestodb.github.io/docs/0.172/index.html).

When you run the QuickStart, it will import every single pricing file providing by AWS *partitioned by region.*  This should keep most queries fairly inexpensive.


## Pricing
Athena costs $5/TB data scanned. The largest data sets are EC2 for each region--approximately 50GB per file.

1TB = 1,000,000 MB = $5

1MB = $5 / 1,000,000 = $0.000005

The largest data set will cost $0.000025 per query.

## Requirements
* Python 3



## QuickStart
1. Clone this repo
2. Install the packages:
    ````
    $ . ./venv/bin/activate
    $ pip install -r ./requirements.txt
    ````

3. Run the importer:
    ````
    $ python ./main.py
    ````

After the import is complete, you can go to Athena and run queries in each location, for example:

````
SELECT * FROM "awspricedatabase"."awsconfig" WHERE location = 'US East (N. Virginia)'
````

