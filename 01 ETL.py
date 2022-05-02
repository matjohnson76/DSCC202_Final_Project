# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Strips down the tokens table to only ERC20 tokens. Also, adds pricing information
# MAGIC -- Only tracks tokens included in the token_prices_usd table since tokens without pricing info are not of interest to us
# MAGIC -- Only needs to be run once per day so that the token prices are up-to-date
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS tokens_silver;
# MAGIC 
# MAGIC CREATE TABLE tokens_silver
# MAGIC (
# MAGIC   address STRING,
# MAGIC   name STRING,
# MAGIC   symbol STRING,
# MAGIC   price_usd DOUBLE
# MAGIC )
# MAGIC USING delta;
# MAGIC 
# MAGIC -- TODO add index int that can be used in token_transfers_silver to reduce the size?
# MAGIC 
# MAGIC INSERT INTO tokens_silver
# MAGIC   SELECT DISTINCT TPU.contract_address, TPU.name, TPU.symbol, TPU.price_usd
# MAGIC   FROM ethereumetl.token_prices_usd TPU INNER JOIN ethereumetl.tokens T ON contract_address=address
# MAGIC     WHERE asset_platform_id = 'ethereum';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Strips down the token_transfers table to a more manageable set of useful attributes
# MAGIC -- Also removes transfers that involve tokens not stored in the tokens_silver table (see above command)
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS token_transfers_silver;
# MAGIC 
# MAGIC CREATE TABLE token_transfers_silver
# MAGIC (
# MAGIC   token_address STRING,
# MAGIC   from_address STRING,
# MAGIC   to_address STRING,
# MAGIC   value DECIMAL(38,0),
# MAGIC   timestamp TIMESTAMP
# MAGIC )
# MAGIC USING delta;
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO token_transfers_silver
# MAGIC   SELECT T.address, TT.from_address, TT.to_address, TT.value, CAST(B.timestamp AS TIMESTAMP)
# MAGIC   FROM tokens_silver T, ethereumetl.token_transfers TT, ethereumetl.blocks B
# MAGIC   WHERE T.address = TT.token_address AND TT.block_number = B.number;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Fills the abridged token transfers table given a start date specified in the widget
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS etl_tok_trans_abridged;
# MAGIC 
# MAGIC CREATE TABLE etl_tok_trans_abridged(
# MAGIC   token_address STRING,
# MAGIC   from_address STRING,
# MAGIC   to_address STRING,
# MAGIC   value DECIMAL(38,0),
# MAGIC   timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC INSERT INTO etl_tok_trans_abridged
# MAGIC   SELECT token_address, from_address, to_address, value, timestamp
# MAGIC   FROM token_transfers_silver
# MAGIC   WHERE timestamp > CAST('${start.date}' AS TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- The results below are useful for testing the balance-checking code since the from_addresses are guranteed to within the date range specified
# MAGIC -- Just choose a from_address and input it to the widget
# MAGIC 
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC SELECT from_address, timestamp
# MAGIC FROM etl_tok_trans_abridged
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Fills the tokens sold table for the specified wallet address
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS etl_toks_sold;
# MAGIC 
# MAGIC CREATE TABLE etl_toks_sold(
# MAGIC   token_address STRING,
# MAGIC   amt_sold DECIMAL(38,0)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC INSERT INTO etl_toks_sold
# MAGIC   SELECT token_address, SUM(value)
# MAGIC   FROM etl_tok_trans_abridged
# MAGIC   WHERE from_address = '${wallet.address}'
# MAGIC   GROUP BY token_address;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Fills the tokens bought table for the specified wallet address
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS etl_toks_bought;
# MAGIC 
# MAGIC CREATE TABLE etl_toks_bought(
# MAGIC   token_address STRING,
# MAGIC   amt_bought DECIMAL(38,0)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC INSERT INTO etl_toks_bought
# MAGIC   SELECT token_address, SUM(value)
# MAGIC   FROM etl_tok_trans_abridged
# MAGIC   WHERE to_address = '${wallet.address}'
# MAGIC   GROUP BY token_address;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CALEB
# MAGIC -- Displays the given wallet's token balance for the period
# MAGIC 
# MAGIC USE g04_db;
# MAGIC 
# MAGIC SELECT B.token_address AS Buy_Tok,
# MAGIC        B.amt_bought, 
# MAGIC        S.token_address AS Sell_Tok, 
# MAGIC        S.amt_sold, 
# MAGIC        (CASE WHEN S.amt_sold IS NULL THEN B.amt_bought 
# MAGIC              WHEN B.amt_bought IS NULL THEN -S.amt_sold
# MAGIC         ELSE B.amt_bought - S.amt_sold END) AS period_balance
# MAGIC FROM etl_toks_bought B FULL OUTER JOIN etl_toks_sold S ON B.token_address = S.token_address;
# MAGIC 
# MAGIC -- Some balances may be negative because the tracking period does not necessarily start from the beginning of time
# MAGIC -- For example, I set my start date to '2022-01-01'. When I run this code, there are usually a few tokens that come up with
# MAGIC -- negative balances. This is almost certainly because the owner of the wallet bought some tokens of that type before 2022-01-01
# MAGIC -- and only sold them afterwards.
# MAGIC -- NOTE: I haven't yet tested this code with a start date corresponding to the first erc20 transaction
# MAGIC 
# MAGIC -- address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';
# MAGIC -- Wrapped Ether (see addr above) is almost always negative

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS g04_db.walletTest3;
# MAGIC CREATE TABLE g04_db.walletTest3(
# MAGIC   wallet_hash string,
# MAGIC   token_address string,
# MAGIC   total_sold decimal(38,0),
# MAGIC   total_bought decimal(38,0),
# MAGIC   active_holding decimal(38,0),
# MAGIC   price_usd float,
# MAGIC   tokens_sold_usd float,
# MAGIC   tokens_bought_usd float,
# MAGIC   active_holding_usd float
# MAGIC   )
# MAGIC   USING delta
# MAGIC   PARTITIONED BY (wallet_hash)
# MAGIC   LOCATION "/mnt/dscc202-datasets/misc/G04/tokenrec/tables/walletTest3";
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW walletTemp
# MAGIC  AS
# MAGIC   SELECT
# MAGIC       tBuys.to_address as wallet_hash,
# MAGIC       tBuys.token_address,
# MAGIC       SUM(tSells.value) as tokens_sold,
# MAGIC       SUM(tBuys.value) as tokens_bought,
# MAGIC       SUM(tSells.value)-SUM(tBuys.value) as active_holding
# MAGIC 
# MAGIC     FROM ethereumetl.token_transfers tBuys
# MAGIC     left outer join ethereumetl.token_transfers tSells on tBuys.to_address = tSells.from_address and tBuys.token_address = tSells.token_address
# MAGIC     where tBuys.block_number in (13641878,13641879) and tSells.block_number in (13641878,13641879)
# MAGIC     GROUP BY tBuys.from_address,tBuys.token_address;
# MAGIC 
# MAGIC INSERT INTO g04_db.walletTest3
# MAGIC   select distinct
# MAGIC   wlts.wallet_hash,
# MAGIC   wlts.token_address,
# MAGIC   wlts.tokens_sold,
# MAGIC   wlts.tokens_bought,
# MAGIC   wlts.active_holding,
# MAGIC   tpu.price_usd,
# MAGIC   wlts.tokens_sold * tpu.price_usd as tokens_sold_usd,
# MAGIC   wlts.tokens_bought * tpu.price_usd as tokens_bought_usd,
# MAGIC   wlts.active_holding * tpu.price_usd as active_holding_usd
# MAGIC   
# MAGIC   from walletTemp wlts
# MAGIC   left outer join ethereumetl.tokens t on t.address = wlts.token_address
# MAGIC   left outer join ethereumetl.token_prices_usd tpu on tpu.name = t.name

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS g04_db.Wallets;
# MAGIC CREATE TABLE g04_db.Wallets(
# MAGIC   wallet_hash string,
# MAGIC   token_address string,
# MAGIC   active_holding_usd float
# MAGIC   )
# MAGIC   USING delta
# MAGIC   PARTITIONED BY (wallet_hash)
# MAGIC   LOCATION "/mnt/dscc202-datasets/misc/G04/tokenrec/tables/Wallets";
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW walletTemp
# MAGIC  AS
# MAGIC   SELECT
# MAGIC       tBuys.to_address as wallet_hash,
# MAGIC       tBuys.token_address,
# MAGIC       SUM(tBuys.value)-SUM(tSells.value) as active_holding
# MAGIC 
# MAGIC     FROM ethereumetl.token_transfers tBuys
# MAGIC     left outer join ethereumetl.token_transfers tSells on tBuys.to_address = tSells.from_address and tBuys.token_address = tSells.token_address
# MAGIC     where tBuys.block_number in (13641878,13641879) and tSells.block_number in (13641878,13641879)
# MAGIC     GROUP BY tBuys.to_address,tBuys.token_address;
# MAGIC 
# MAGIC INSERT INTO g04_db.Wallets
# MAGIC   select distinct
# MAGIC   wlts.wallet_hash,
# MAGIC   wlts.token_address,
# MAGIC   wlts.active_holding * tpu.price_usd as active_holding_usd
# MAGIC   
# MAGIC   from walletTemp wlts
# MAGIC   left outer join ethereumetl.tokens t on t.address = wlts.token_address
# MAGIC   left outer join ethereumetl.token_prices_usd tpu on tpu.name = t.name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from g04_db.wallets

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
