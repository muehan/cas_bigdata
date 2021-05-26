%spark.ipyspark

from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType, BooleanType, LongType
from pyspark.sql.functions import year, month, col, sum, min, count

# ----- Tenant ------
tenantSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("LanguageId", IntegerType()), \
    StructField("Code", StringType()), \
    StructField("ShortName", StringType()), \
    StructField("LongName", StringType()), \
    StructField("IsActive", BooleanType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("Address1", StringType()), \
    StructField("Address2", StringType()), \
    StructField("Zip", StringType()), \
    StructField("City", StringType()), \
    StructField("Lf10Path", StringType()), \
    StructField("Lf20Path", StringType()), \
    StructField("Lf10ArchivePath", StringType()), \
    StructField("Lf20ArchivePath", StringType()), \
    StructField("Lf10FailedPath", StringType()), \
    StructField("Lf20FailedPath", StringType()) \
    ])

tenant_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/tenant.csv", header=True, schema=tenantSchema)

# ----- Supplier ------
supplierSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("IsActive", BooleanType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("GlobalNo", IntegerType()), \
    StructField("No", IntegerType()), \
    StructField("Caption", StringType()), \
    StructField("Zip", IntegerType()), \
    StructField("City", StringType()) \
    ])

supplier_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/supplier.csv", header=True, schema=supplierSchema)
    
    # Order
orderSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("CollectiveOrderId", IntegerType()), \
    StructField("SupplierId", IntegerType()), \
    StructField("CustomerId", IntegerType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("No", IntegerType()), \
    StructField("DeliveryDateTime", TimestampType()) \
    ])

order_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/order.csv", header=True, schema=orderSchema)
    
    
orderCriterionLinkSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("CollectiveOrderId", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("CriterionAssortmentLinkId", IntegerType()), \
    StructField("PerformanceTypeId", IntegerType()), \
    StructField("ArticleId", IntegerType()), \
    StructField("TargetQuantity", IntegerType()), \
    StructField("ActualQuantity", IntegerType()), \
    StructField("ConsumerUnit", StringType()), \
    StructField("CriterionDeduction", IntegerType()), \
    StructField("EffectiveDeduction", IntegerType()), \
    StructField("PercentAffected", IntegerType()), \
    StructField("ReductionFactor", IntegerType()), \
    StructField("Performance", IntegerType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    ])

orderCriterionLink_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/OrderCriterionLink.csv", header=True, schema=orderCriterionLinkSchema)

# CollectiveOrder
collectiveOrderSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("No", StringType()), \
    StructField("CommentLp", StringType()), \
    StructField("DocumentId", IntegerType()) \
    ])

collectiveOrder_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/collectiveorder.csv", header=True, schema=collectiveOrderSchema)
    
order_month_year = order_df \
    .withColumn("Month", month("DeliveryDateTime")) \
    .withColumn("Year", year("DeliveryDateTime")) \
    
order_from_supplier_by_year_and_month = order_month_year.alias("ord") \
    .join(supplier_df.alias("sup"), order_month_year.SupplierId == supplier_df.Id) \
    .filter("sup.No == '92004621'")

#order_from_supplier_by_year_and_month.show()
order_from_supplier_by_year_and_month.cache()

orderNumbersFromMonth_df = order_from_supplier_by_year_and_month \
    .filter("Year == 2021") \
    .filter("Month == 03") \
    .select( \
        col("ord.No").alias("OrderNo"), \
        col("ord.Id").alias("OrderId"), \
        col("ord.CollectiveOrderId").alias("CollectiveOrderId"))
    
#orderNumbersFromMonth_df.show()

criterions_df = collectiveOrder_df.alias("col") \
    .join(orderCriterionLink_df.alias("ocl"), collectiveOrder_df.Id == orderCriterionLink_df.CollectiveOrderId) \
    .join(orderNumbersFromMonth_df.alias("ordNum"), collectiveOrder_df.Id == orderNumbersFromMonth_df.CollectiveOrderId) \
    .groupBy("ocl.PerformanceTypeId") \
    .agg(
        sum("ocl.CriterionDeduction").alias("TotalCriterionDeduction"), \
        sum("ocl.EffectiveDeduction").alias("TotalEffectiveDeduction"))
    
    
criterions_df.show()

# PerformanceType LP = 1
# PerformanceType MP = 2
# PerformanceType PP = 3
