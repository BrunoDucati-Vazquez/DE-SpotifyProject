import dlt

@dlt.table()
def dimDate_stg():
    df = spark.readStream.table("spotify.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
    target = "dimdate",
    source = "dimDate_stg",
    keys = ["date_key"],
    sequence_by = "date",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    name = None,
    once = False
)