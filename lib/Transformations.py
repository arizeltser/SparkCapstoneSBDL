from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, \
    collect_list

def get_insert_operation(col_name, value):
    return struct (
            lit("INSERT").alias("operation"),
            value.alias("newValue"),
            lit(None).alias("oldValue")
        ).alias(col_name)



def account_to_contract(df):
    
    contract_titles = array(
        when(~isnull(col("legal_title_1")), struct(
                lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                col("legal_title_1").alias("contractTitleLine")
            )).otherwise(lit(None)),
        when(~isnull(col("legal_title_2")), struct(
                lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                col("legal_title_2").alias("contractTitleLine")
            )).otherwise(lit(None))
    )

    contract_titles = filter(contract_titles, lambda x: ~isnull(x))


    tax_info = struct(
        col("tax_id_type").alias("taxIdType"),
        col("tax_id").alias("taxId")
    )

    result_df = df.select(
        col("account_id"),
        get_insert_operation("contractIdentifier", col("account_id")),
        get_insert_operation("sourceSystemIdentifier", col("source_sys")),
        get_insert_operation("contactStartDateTime", col("account_start_date")),
        get_insert_operation("contractTitle", contract_titles),
        get_insert_operation("taxIdentifier", tax_info),
        get_insert_operation("contractBranchCode", col("branch_code")),
        get_insert_operation("contractCountry", col("country"))
    )

    return result_df



def get_party_relations(df):
    return df.select("account_id", "party_id",
                     get_insert_operation("partyIdentifier", col("party_id")),
                     get_insert_operation("partyRelationshipType", col("relation_type")),
                     get_insert_operation("partyRelationStartDateTime", col("relation_start_date"),)
                     )


def get_party_addresses(df):
    
    address_info = struct(
        col("address_line_1").alias("addressLine1"),
        col("address_line_2").alias("addressLine2"),
        col("city").alias("addressCity"),
        col("postal_code").alias("addressPostalCode"),
        col("country_of_address").alias("addressCountry"),
        col("address_start_date").alias("addressStartDate")
    )
    
    result_df = df.select(
        col("party_id"),
        get_insert_operation("partyAddress", address_info),
    )

    return result_df



def join_party_addresses(party, addresses):


    return party.join(addresses, "party_id", "left_outer") \
        .groupBy("account_id") \
        .agg(collect_list(struct("partyIdentifier",
                                 "partyRelationshipType",
                                 "partyRelationStartDateTime",
                                 "partyAddress"
                                 ).alias("partyDetails")
                          ).alias("partyRelations"))

    





def join_parties(contract, parties):
    join_expr = contract["account_id"] == parties["account_id"]
    join_type = "left_outer"
    return contract.join(parties, join_expr, join_type).drop(parties["account_id"])



def attach_header(spark, df):
    
    header_info = [("SBDL-Contract", 1, 0), ]
    header_df = spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")

    event_df = header_df.hint("broadcast").crossJoin(df) \
        .select(struct(expr("uuid()").alias("eventIdentifier"),
                       col("eventType"), col("majorSchemaVersion"), col("minorSchemaVersion"),
                       lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
                       ).alias("eventHeader"),
                array(struct(lit("contractIdentifier").alias("keyField"),
                             col("account_id").alias("keyValue")
                             )).alias("keys"),
                struct(col("contractIdentifier"),
                       col("sourceSystemIdentifier"),
                       col("contactStartDateTime"),
                       col("contractTitle"),
                       col("taxIdentifier"),
                       col("contractBranchCode"),
                       col("contractCountry"),
                       col("partyRelations")
                       ).alias("payload")
                )

    return event_df
    
    
