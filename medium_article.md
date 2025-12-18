# Sans titre

Starting around 6 months ago my LinkedIn and Reddit feeds started popping off with posts about [dlt](https://github.com/dlt-hub/dlt),
 an open source Python library for data extraction and loading. Though I
 had used more established ETL tools such as Fivetran, Meltano, and 
Airbyte before, the posts from data engineers describing how fast and 
easy it was to set up a data pipeline using dlt caught my attention. And
 so I decided to give it a shot and try building a dlt source (which I 
lazily named [`dune-lt`](https://github.com/swang2016/dune-lt)) to extract crypto data from [Dune’s](https://dune.com/) REST API and load it into DuckDB and Snowflake data warehouses.

This blog post will cover the following:

- Main components of `dune-lt`
- Example 1: Extracting and Loading Daily DEX Trading Volume
- Example 2: Incrementally Extracting and Loading Data
- Example 3: Extracting Data Using Custom SQL
- Dynamically generating `dlt` resources based off a query config file
- Loading data into Snowflake

If you just want to see the code, you can check out the Github repo [here](https://github.com/swang2016/dune-lt).

## The 3 Main Components of `dune-lt` :

- [Dune REST API](https://docs.dune.com/api-reference/overview/introduction): Dune is a blockchain analytics platform that allows users to query and visualize blockchain data.
- [dlt](https://github.com/dlt-hub/dlt): Python library that provides a framework for interacting with APIs to
extract data. It also has pre-built connectors for loading data into
destinations such as Snowflake, DuckDB, and Iceberg.
- [Spice](https://github.com/paradigmxyz/spice/tree/main): The secret ingredient. The giga-brains over at [Paradigm](https://www.paradigm.xyz/), a crypto VC, built a fantastic Dune REST API client. I could have
directly called the Dune API but Spice offers several quality of life
improvements such as automatically handling pagination, automatically
executing queries that have no existing executions, and auto-retrying
requests when encountering rate limits.

The
 heavy lifting is handled by the above 3 projects; I merely wrapped Dune
 API calls with the Spice client in a dlt source so all credit goes to 
the teams that built these tools!

## Example 1: Extracting and Loading Daily DEX Trading Volumes

The
 following Dune query pulls the daily USD trading volume from a variety 
of DEXs (decentralized exchanges) over the last 30 days: [https://dune.com/queries/4388](https://dune.com/queries/4388):

```
SELECT
  project,
  DATE_TRUNC('day', block_time),
  SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
FROM
  dex."trades" AS t
WHERE
 block_time > DATE_TRUNC('day', NOW()) - INTERVAL '30' day
  AND block_time < DATE_TRUNC('day', NOW())
GROUP BY
  1,
  2
```

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*-BbKWikXnlM86gbHj10GQw.png)

DEX volume query in Dune

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*S12M9gOt5iVC4l2Tu_JQmg.png)

DEX volume visualized in Dune

The following code creates a dlt resource called `dex_volume` (you can think of a resource as a single table to load extracted data into) and a dlt source called `dune_source` that calls resources when the pipeline is run:

```
import dlt
import spice

@dlt.resource(
    name="dex_volume", # name the target table
    primary_key=["project", "_col1"],
    write_disposition="merge"
)
def dex_volume(
        api_key: str = dlt.secrets.value, # saved in .dlt/secrets.toml
):
    df = spice.query(
        "https://dune.com/queries/4388", # pass in Dune query URL here
        api_key=api_key,
        refresh=True,
        cache=False # Spice can cache queries if set to True
    )
    yield df.to_dicts()

@dlt.source
def dune_source():
    return [dex_volume]

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="dune_source", destination="duckdb", dataset_name="dune_queries"
    )
    load_info = pipeline.run(dune_source())
    print(load_info)
```

A few things to note:

- **Dune API key:** `dlt` [handles secrets](https://dlthub.com/docs/walkthroughs/add_credentials) by storing and retrieving them from a directory in the base of your project directory, `.dlt/secrets.toml` .

```
your_project/
├── .dlt/
│   └── secrets.toml # Sensitive information such as API keys and credentials
```

In this example the `secrets.toml` file might look like this:

```
[dune_source]
api_key = "YOUR_DUNE_API_KEY"
```

- **Primary key:** primary key(s) can be defined in the resource. This will help deduplicate data when loading into a table
- [**Write Disposition:](https://dlthub.com/docs/general-usage/incremental-loading#choosing-a-write-disposition)** How new data should be loaded into a table. Possible values are merge (upsert), append, or replace.

To run this pipeline and load the DEX volume query results into a local DuckDB database called `dune_source.duckdb`, run `python my_pipeline.py`(replace `my_pipeline.py` with the name of your file):

We can examine the loaded data:

```
import duckdb
db = duckdb.connect("dune_source.duckdb")

db.execute("select * from dune_queries.dex_volume order by _col1").fetchdf()
```

Which should yield a table looking like this:

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*Mfa21Lu6AQRhqVSOVoKOig.png)

**Important:** Remember to close the DuckDB connection with `db.close()` before running the pipeline again. DuckDB only supports a single reader/writer at a time.

## Example 2: Incrementally Extracting and Loading Daily DEX Trading Volumes

The
 above example is great but what if we want to incrementally extract and
 load DEX volume data? Doing so reduces the processing time of each 
subsequent query, which is important when it comes to [budgeting credits on Dune](https://dune.com/pricing), and also reduces the amount of data we need to load into our target destination each time.

[Become a member](https://miro.medium.com/v2/da:true/resize:fit:0/60026f4340686a391639ac58864da18070aa773cea45de6e55fa47fd56bfdb74)

To allow for incremental queries, I forked the original query on Dune so that it can accept a date parameter:

[https://dune.com/queries/4778954](https://dune.com/queries/4778954)

```
SELECT
  project,
  DATE_TRUNC('day', block_time) as date,
  SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
FROM
  dex."trades" AS t
WHERE
 block_time >= TIMESTAMP '{{date}}'
GROUP BY
  1,
  2
```

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*8yzP1wboGOVUL1ABQh7p8w.png)

Forked DEX volume Dune query with data paramater

Let’s modify the dlt resource to be able to handle incrementally extracting and loading this query’s data:

```
import dlt
import spice

@dlt.resource(
    name="dex_volume_incremental",
    primary_key=["project", "date"], # notice we renamed _col1 to date in the forked Dune query
    write_disposition="merge"
)
def dex_volume_incremental(
        api_key: str = dlt.secrets.value,
        cursor=dlt.sources.incremental(cursor_path="date", initial_value="2025-01-01")
):
    df = spice.query(
        "https://dune.com/queries/4778954",
        api_key=api_key,
        refresh=True,
        cache=False,
        parameters={"date": cursor.last_value} # param to incrementally run the query
    )
    yield df.to_dicts()

@dlt.source
def dune_source():
    return [dex_volume_incremental]

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="dune_source", destination="duckdb", dataset_name="dune_queries"
    )
    load_info = pipeline.run(dune_source())
    print(load_info)
```

Things to note:

- **Incremental cursor:** we set the cursor argument in the `dex_volume_incremental` resource with a [dlt cursor object](https://dlthub.com/docs/general-usage/incremental-loading#incremental-loading-with-a-cursor-field). `cursor_path` is the name of the column in the Dune query that will be used to filter results on incrementally. `initial_value` is the starting value to filter on.

```
cursor=dlt.sources.incremental(cursor_path="date", initial_value="2025-01-01")
```

- **Dune Query parameter:** we use the Spice client’s `parameter` argument to pass in a dictionary of query params. Notice that the key, `date`, matches the name of the parameter in the Dune query.
- **State tracking:** an awesome feature of dlt is that it tracks the state of incremental loads in the destination table itself without the need for an external
state-tracking store or database. In this example, it would check for
the max value of the `date` column in the destination table `dune_queries.dex_volume_incremental` .

If we run the pipeline again: `python my_pipeline.py` we should see a new table loaded into our DuckDB database called `dune_queries.dex_volume_incremental` . If we examine that table:

```
import duckdb
db = duckdb.connect("dune_source.duckdb")

db.execute("select * from dune_queries.dex_volume_incremenal order by date").fetchdf()
```

It should look like this:

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*2A6F97OOUxBkIvOGPZ-PfQ.png)

Notice that the first date in the data is `2025-01-01` , which matches the initial value we set in the cursor. Follow up pipeline runs will only query data where `date >= SELECT MAX(date) FROM dex_volume_incremental`

## Example 3: Extracting Data Using Custom SQL

If
 you don’t want to reference a Dune URL or query id and instead want to 
define a resource using custom SQL, you can do that too since `spice` supports SQL as an input into its `.query()` method.

The following dlt resource shows how you can do this:

```
import dlt
import spice

@dlt.resource(
    name="custom_sql",
    primary_key="timestamp",
    write_disposition="merge"
)
def custom_sql(
        api_key: str = dlt.secrets.value,
        cursor=dlt.sources.incremental(cursor_path="timestamp", initial_value="2024-11-01")
):
    df = spice.query(
        f"""
        SELECT
            timestamp,
            blockchain,
            contract_address,
            symbol,
            price
        FROM prices.day
        WHERE symbol = 'BRETT'
        AND blockchain = 'base'
        AND {cursor.cursor_path} > TIMESTAMP '{cursor.last_value}'
        order by timestamp
        """,
        api_key=api_key,
        refresh=True,
        cache=False,
    )
    yield df.to_dicts()

@dlt.source
def dune_source():
    return [custom_sql]

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="dune_source", destination="duckdb", dataset_name="dune_queries"
    )
    load_info = pipeline.run(dune_source())
    print(load_info)
```

Things to note:

- We are querying from the `prices.day` table in Dune where the symbol is `BRETT` and the token is on Base chain.
- We inject the incremental cursor object’s `cursor.cursor_path` and `cursor.last_value` values into the SQL query itself

If we run the pipeline again we will see a new table called `dune_queries.custom_sql`loaded in our `dune_source.duckdb` database:

Press enter or click to view image in full size

![](https://miro.medium.com/v2/resize:fit:700/1*oIAl29R5wghW0SoxA5CCAg.png)

## Dynamically Generating dlt Resources

In the previous 3 examples, we manually defined a `dlt` resource for each Dune query. For each query we had to define the following attributes:

- A Dune query URL, id, or SQL
- Primary key(s) (Optional)
- Write disposition: merge, append, or replace
- Query parameters (Optional, required if the Dune query has params)
- A replication key and starting value if doing incremental loads

What if instead of manually creating a resource for each query, we could throw these attributes into a config file and have the `dlt` resources be dynamically generated? That’s exactly what [`dune-lt`](https://github.com/swang2016/dune-lt/tree/main) seeks to do.

In your `dlt` project we can create a `config.toml` file in the same directory as your `secrets.toml` file, `.dlt/config.toml` .

```
your_project/
├── .dlt/
│   ├── config.toml # Configuration settings for your dlt project
│   └── secrets.toml # Sensitive information such as API keys and credentials
```

In the config file we can lay out the same 3 example queries like so:

```
########## Example 1: DEX Volume ##########
[dune_queries.dex_volume]
query = "https://dune.com/queries/4388"
primary_key = ["project", "_col1"]
write_disposition = "merge"

##### Example 2: DEX Volume With Incremental #####
[dune_queries.dex_volume_incremental]
query = "https://dune.com/queries/4778954"
primary_key = ["project", "date"]
write_disposition = "merge"
replication_key = "date" # make sure the name of the param in dune query matches name of the replication key
starting_replication_value = "2025-01-01"

##### Example 3: Custom SQL #####
[dune_queries.custom_sql]
query = """
SELECT
    timestamp,
    blockchain,
    contract_address,
    symbol,
    price
FROM prices.day
WHERE symbol = 'BRETT'
AND blockchain = 'base'
AND contract_address = from_hex('532f27101965dd16442e59d40670faf5ebb142e4')
-- Must use 'replication_key' and 'cursor_value' keywords for incremental loading
AND {replication_key} > TIMESTAMP '{cursor_value}'
order by timestamp
"""
primary_key = "timestamp"
write_disposition = "merge"
replication_key = "timestamp"
starting_replication_value = "2024-11-01"
```

In the `dune-lt` repo, I’ve added functions to dynamically generate resources based off queries specified in the `config.toml` file. You can check out the logic for those [here](https://github.com/swang2016/dune-lt/blob/main/dune_lt/__init__.py).

We can run a pipeline with those dynamically generated resources by importing the`dune_source` function from `dune-lt` and adding just a few lines of pipeline code:

```
from dune_lt import dune_source # dynamically generates resources based off config.toml
import dlt

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="dune_source", destination="duckdb", dataset_name="dune_queries"
    )
    load_info = pipeline.run(dune_source())
    print(load_info)
```

If
 we run this pipeline, it will create and load the same 3 tables as 
before into a local DuckDB database. Instead of writing a separate 
resource for each query as we did before, now we can just specify each 
query in a config file and have the `dune_source` function dynamically create resources for each query. Check out the [`dune-lt`](https://github.com/swang2016/dune-lt) Github repo if you want to play around with it more.

## Loading Data Into Snowflake

We
 can also load data into Snowflake using dlt’s Snowflake destination 
connector. To set this up, put your Snowflake credentials into your `.dlt/secrets.toml` file like so:

```
[dune_source]
api_key = "YOUR_DUNE_API_KEY"

[destination.snowflake.credentials]
database = "<your-database>"
password = "<your-password>"
username = "<your-username>"
host = "<your-host>"
warehouse = "<your-warehouse>"
role = "<your-role>"
```

To
 load the Dune query data into Snowflake, we can modify the pipeline 
code slightly (notice we are re-using the dynamically generated 
resources from the`dune_source` function):

```
from dune_lt import dune_source # dynamically generates resources based off config.toml
import dlt

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="dune_source", destination="snowflake", dataset_name="dune_queries"
    )
    load_info = pipeline.run(dune_source())
    print(load_info)
```

After running this pipeline we can take a look at one of the tables, `dex_volume`, in Snowflake: