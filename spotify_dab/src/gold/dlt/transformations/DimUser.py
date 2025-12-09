import dlt

# 1️⃣ Define expectations ONCE at the top
expectations = {
    "valid_user_id": "user_id IS NOT NULL",
    "valid_user_name": "user_name IS NOT NULL"
}

# 2️⃣ Staging streaming table with expectations
@dlt.table(name="dimuser_stg")
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    return spark.readStream.table("spotify_cata.silver.dimuser")

# 3️⃣ Target streaming table for SCD2 (no expectations here)
#    This is the table that AUTO CDC will maintain.
dlt.create_streaming_table(
    name="dimuser",                    # 👈 must match target below
    comment="SCD2 dimuser table"
)

# 4️⃣ AUTO CDC flow that writes into the target table above
dlt.create_auto_cdc_flow(
    target="dimuser",                  # 👈 same name as create_streaming_table
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",          # make sure this column exists
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name="dimuser_cdc_flow",
    once=False
)
