# %spark.ipyspark

from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType, BooleanType, LongType
from pyspark.sql.functions import col, to_date, sum

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

# Order Criteriion Link
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
    
# Article
articleSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("AssortmentId", IntegerType()), \
    StructField("ArticleGroupId", IntegerType()), \
    StructField("IsActive", BooleanType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("No", StringType()), \
    StructField("Caption", StringType()), \
    StructField("Type", IntegerType()), \
    StructField("LowerToleranceValue", IntegerType()), \
    StructField("UpperToleranceValue", IntegerType()), \
    StructField("EffectiveLowerToleranceValue", IntegerType()), \
    StructField("EffectiveLowerToleranceType", IntegerType()), \
    StructField("EffectiveUpperToleranceValue", IntegerType()), \
    StructField("EffectiveUpperToleranceType", IntegerType()), \
    StructField("Comment", IntegerType()) \
    ])

article_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/article.csv", header=True, schema=articleSchema)

#Criterion Assortment Link
criterionAssortmentLinkSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("CriterionId", IntegerType()), \
    StructField("AssortmentId", IntegerType()), \
    StructField("Deduction", IntegerType()), \
    StructField("IsActive", BooleanType()) \
    ])

criterionAssortmentLink_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/CriterionAssortmentLink.csv", header=True, schema=criterionAssortmentLinkSchema)

#Position
positionSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("AssortmentId", IntegerType()), \
    StructField("OrderId", IntegerType()), \
    StructField("ArticleId", IntegerType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("TargetTradeUnitQuantity", IntegerType()), \
    StructField("TradeUnit", StringType()), \
    StructField("TargetConsumerUnitQuantity", IntegerType()), \
    StructField("ConsumerUnit", StringType()), \
    StructField("ActualTradeUnitQuantity", IntegerType()), \
    StructField("ActualConsumerUnitQuantity", IntegerType()), \
    StructField("ModeOfShipment", DoubleType()), \
    StructField("OrderPositionNo", IntegerType()), \
    StructField("HasDeliveries", BooleanType()), \
    StructField("IsEvaluated", BooleanType()), \
    StructField("DeliveryWithoutOrder", BooleanType()), \
    StructField("CommentMp", StringType()), \
    StructField("CommentPp", StringType()), \
    StructField("QuantitiesModifiedByUser", StringType()), \
    StructField("SubcontractorGgn", StringType()), \
    StructField("SubcontractorName", StringType()) \
    ])

position_df = spark \
    .read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .schema(positionSchema) \
    .csv("hdfs://bd-1:9000/lfs/position_new.csv")
    
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
    
    
criterionSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("IsActive", BooleanType()), \
    StructField("Code", StringType()), \
    StructField("QuantityPerformanceType", IntegerType()), \
    StructField("OnlyEditableByPowerUser", BooleanType()), \
    StructField("FullDeliveryAffected", BooleanType()), \
    ])

criterion_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/criterion.csv", header=True, schema=criterionSchema)
    
criterionTextSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("LanguageId", IntegerType()), \
    StructField("CriterionId", IntegerType()), \
    StructField("Caption", StringType()), \
    ])

criterionText_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/criterionText.csv", header=True, schema=criterionTextSchema)
    
assortmentSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("IsActive", BooleanType()), \
    StructField("CreatedAt", TimestampType()), \
    StructField("CreatedBy", StringType()), \
    StructField("ModifiedAt", TimestampType()), \
    StructField("ModifiedBy", StringType()), \
    StructField("Code", StringType()), \
    ])

assortment_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/assortment.csv", header=True, schema=assortmentSchema)

position_sums_df = collectiveOrder_df.alias("co") \
    .join(order_df.alias("ord"), collectiveOrder_df.Id == order_df.CollectiveOrderId) \
    .join(position_df.alias("pos"), position_df.OrderId == order_df.Id) \
    .groupBy("co.Id", "co.TenantId", "ord.DeliveryDateTime") \
    .agg( \
        sum("pos.ActualTradeUnitQuantity").alias("ActualTradeUnitQuantity"), \
        sum("pos.TargetTradeUnitQuantity").alias("TargetTradeUnitQuantity") \
    ) \
    .select(col("Id").alias("coid"), col("co.TenantId").alias("TenantId"), col("ord.DeliveryDateTime").alias("coDeliveryDateTime"), col("ActualTradeUnitQuantity").alias("coActualTradeUnitQuantity"),  col("TargetTradeUnitQuantity").alias("coTargetTradeUnitQuantity"))

joined = supplier_df.alias("sup") \
    .join(tenant_df.alias("t"), supplier_df.TenantId == tenant_df.Id) \
    .join(order_df.alias("ord"), order_df.SupplierId == supplier_df.Id) \
    .join(position_df.alias("pos"), position_df.OrderId == order_df.Id) \
    .join(assortment_df.alias("asPos"), assortment_df.Id == position_df.AssortmentId) \
    .join(collectiveOrder_df.alias("co"), collectiveOrder_df.Id == order_df.CollectiveOrderId) \
    .join(orderCriterionLink_df.alias("ocl"), orderCriterionLink_df.CollectiveOrderId == collectiveOrder_df.Id) \
    .join(article_df.alias("art"), article_df.Id == orderCriterionLink_df.ArticleId) \
    .join(criterionAssortmentLink_df.alias("cal"), criterionAssortmentLink_df.Id == orderCriterionLink_df.CriterionAssortmentLinkId) \
    .join(criterion_df.alias("cr"), criterion_df.Id == criterionAssortmentLink_df.CriterionId) \
    .join(criterionText_df.alias("ct"), criterionText_df.CriterionId == criterion_df.Id) \
    .join(position_sums_df.alias("posSum"), position_sums_df.coid == collectiveOrder_df.Id) \
    .filter("ord.DeliveryDateTime >= date'2021-03-05'") \
    .filter("ord.DeliveryDateTime < date'2021-03-06'") \
    .filter("t.Code == '007'") \
    .filter("sup.No == '92004621'") \
    .filter("ct.LanguageId == 1") \
    .select("sup.No", "ord.DeliveryDateTime", "co.Id", "co.No", "art.No", "art.Caption", "cr.Code", "ct.Caption", "posSum.coActualTradeUnitQuantity", "posSum.coTargetTradeUnitQuantity", "ocl.ActualQuantity", "ocl.TargetQuantity", "ocl.ConsumerUnit", "ocl.CriterionDeduction", "ocl.EffectiveDeduction", "ocl.Performance", "ocl.TenantId", "ocl.ReductionFactor", "ocl.ModifiedBy", "ocl.CreatedBy")

joined.dropDuplicates().show(300)
