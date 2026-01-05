import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# 1. Initialization (CRITICAL FOR BOOKMARKS)
# Bookmarks track state based on the JOB_NAME and run ID.
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==========================================
# STEP 2: DEFINE YOUR "TABLE VARIABLE"
# ==========================================
tables_to_process = [
    {
        "table_name": "transactions",
        "source_path": "s3://my-bank-datalake-dev/source/raw_json_landing/transactions/",
        "target_path": "s3://my-bank-datalake-dev/source/bronze/transactions/"
    },
    {
        "table_name": "accounts",
        "source_path": "s3://my-bank-datalake-dev/source/raw_json_landing/accounts/",
        "target_path": "s3://my-bank-datalake-dev/source/bronze/accounts/"
    },
    {
        "table_name": "customers",
        "source_path": "s3://my-bank-datalake-dev/source/raw_json_landing/customers/",
        "target_path": "s3://my-bank-datalake-dev/source/bronze/customers/"
    },
    {
        "table_name": "cust_acct",
        "source_path": "s3://my-bank-datalake-dev/source/raw_json_landing/cust_acct/",
        "target_path": "s3://my-bank-datalake-dev/source/bronze/cust_acct/"
    }
]

# ==========================================
# STEP 3: THE LOOP
# ==========================================
for table_config in tables_to_process:
    
    current_table = table_config["table_name"]
    print(f"--- Starting Process for: {current_table} ---")

    # A. Dynamic Read with BOOKMARKS enabled
    # 'transformation_ctx' tells Glue where to look for the checkpoint for THIS specific table.
    source_dyf = glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [table_config["source_path"]],
            "recurse": True,
            "groupFiles": "inPartition",  # Optimization for many small files
        },
        transformation_ctx=f"read_bookmark_{current_table}" 
    )

    # B. Safety Check: If no new files, skip to next table
    if source_dyf.count() == 0:
        print(f"Skipping {current_table} - No new data found.")
        continue

    # ==========================================
    # GLUE NATIVE DATE TRANSFORMATION
    # ==========================================
    
    # 1. Inspect Schema to find date columns
    # We use .schema() to inspect fields without loading all data
    schema_fields = source_dyf.schema().fields
    
    mappings = []
    
    # Build the mapping list dynamically
    for field in schema_fields:
        if "date" in field.name.lower():
            # CAST: Change type to 'date'
            mappings.append((field.name, "string", field.name, "date"))
        else:
            # PASS-THROUGH: Keep everything else as is
            mappings.append((field.name, field.dataType.typeName(), field.name, field.dataType.typeName()))
            
    print(f"[{current_table}] Casting columns using ApplyMapping.")

    # 2. ApplyMapping (Native Glue Transform)
    # This handles the data conversion efficiently within the DynamicFrame
    transformed_dyf = ApplyMapping.apply(
        frame=source_dyf,
        mappings=mappings,
        transformation_ctx=f"map_{current_table}"
    )

    # ==========================================

    # C. Dynamic Write with BOOKMARKS
    # We write the transformed frame. Glue records that we successfully processed 
    # the files associated with 'read_bookmark_{current_table}'
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dyf,
        connection_type="s3",
        connection_options={"path": table_config["target_path"]},
        format="parquet",
        transformation_ctx=f"write_bookmark_{current_table}"
    )
    
    print(f"--- Finished Process for: {current_table} ---")

# 4. Final Commit (REQUIRED)
# This writes the state to the job bookmark service. Without this, 
# the next run will re-process the same files.
job.commit()