import sys
import uuid

from pyspark.sql.functions import struct, col, to_json
from lib import ConfigLoader, Utils, DataLoader, Transformations
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)


    logger.info("Read accounts into DF")
    accounts_df = DataLoader.read_accounts(spark, job_run_env, enable_hive, hive_db)

    logger.info("Transforming accounts into contracts")
    contract_df = Transformations.account_to_contract(accounts_df)
    #contract_df.show(1)

    logger.info("Read parties into DF")
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    parties_df = Transformations.get_party_relations(parties_df)

    logger.info("Read addresses into DF")
    addresses_df = DataLoader.read_addresses(spark, job_run_env, enable_hive, hive_db)
    addresses_df = Transformations.get_party_addresses(addresses_df)
    #addresses_df.show(1)

    logger.info("Joining parties with addresses")
    party_addresses_df = Transformations.join_party_addresses(parties_df, addresses_df)
    #party_addresses_df.show()

    # logger.info("Transforming parties and addresses into party relations")
    # party_relations_df = Transformations.get_party_relations(party_addresses_df)
    # #party_relations_df.show()

    #print(party_relations_df.collect())


    logger.info("Joining party relations with contracts")
    contract_parties_df = Transformations.join_parties(contract_df, party_addresses_df)
    #contract_parties_df.show()

    logger.info("Attaching header to the DF")
    final_df = Transformations.attach_header(spark, contract_parties_df)
    final_df.show()
    print(final_df.collect())

    logger.info("prepping for kafka")
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    #kafka_kv_df.show(1)

    


    # kafka stuff







    logger.info("Finished creating Spark Session")