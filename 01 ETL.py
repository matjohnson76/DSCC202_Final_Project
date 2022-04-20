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
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the price_ table

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
