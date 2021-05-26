%spark.ipyspark

from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType, BooleanType, LongType, FloatType
from pyspark.sql.functions import year, month, col, sum, min, count, when

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
    StructField("TargetTradeUnitQuantity", FloatType()), \
    StructField("TradeUnit", StringType()), \
    StructField("TargetConsumerUnitQuantity", IntegerType()), \
    StructField("ConsumerUnit", StringType()), \
    StructField("ActualTradeUnitQuantity", IntegerType()), \
    StructField("ActualConsumerUnitQuantity", FloatType()), \
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
    
# PerformanceType
performanceTypeSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("Code", StringType()), \
    ])
    
performanceType_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/performanceType.csv", header=True, schema=performanceTypeSchema)
    
# Criterion Assortment Link
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

# Criterion
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
    
# CriterionGroup
criterionGroupSchema = StructType([ \
    StructField("Id", IntegerType()), \
    StructField("TenantId", IntegerType()), \
    StructField("Code", StringType()), \
    ])

criterionGroup_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/CriterionGroup.csv", header=True, schema=criterionGroupSchema)
    
# CriterionGroup
criterionGroupLinkSchema = StructType([ \
    StructField("CriterionGroupId", IntegerType()), \
    StructField("CriterionAssortmentLinkId", IntegerType()) \
    ])

criterionGroupLink_df = spark \
    .read \
    .option("header",True) \
    .option("delimiter", ";") \
    .option("encoding","ISO-8859-1") \
    .csv("hdfs://bd-1:9000/lfs/CriterionGroupLink.csv", header=True, schema=criterionGroupLinkSchema)
    

reportPpQuantities_df = position_df.alias("pos") \
    .join(order_df.alias("ord"), order_df.Id == position_df.OrderId) \
    .join(collectiveOrder_df.alias("col"), collectiveOrder_df.Id == order_df.CollectiveOrderId) \
    .join(orderCriterionLink_df.alias("ocl"), (orderCriterionLink_df.CollectiveOrderId == collectiveOrder_df.Id) & (orderCriterionLink_df.ArticleId == position_df.ArticleId), how='left_outer') \
    .join(performanceType_df.alias("pt"), performanceType_df.Id == orderCriterionLink_df.PerformanceTypeId, how='left_outer') \
    .join(criterionAssortmentLink_df.alias("cal"), criterionAssortmentLink_df.Id == orderCriterionLink_df.CriterionAssortmentLinkId, how='left_outer') \
    .join(criterion_df.alias("crit"), criterion_df.Id == criterionAssortmentLink_df.CriterionId, how='left_outer') \
    .join(criterionGroupLink_df.alias("cgl"), criterionGroupLink_df.CriterionAssortmentLinkId == criterionAssortmentLink_df.Id, how='left_outer') \
    .join(criterionGroup_df.alias("cg"), criterionGroup_df.Id == criterionGroupLink_df.CriterionGroupId, how='left_outer') \
    .filter("(pt.Id is null OR pt.Code == 'PP') AND (ocl.Id is null OR cg.Code = '10')") \
    .select(
          col("col.Id").alias("CollectiveOrderId"), \
          col("ord.Id").alias("OrderId"), \
          col("ord.SupplierId").alias("SupplierId"), \
          col("pos.Id").alias("PositionId"), \
          col("pos.ArticleId").alias("ArticleId"), \
          col("pos.AssortmentId").alias("AssortmentId"), \
          col("ocl.Id").alias("OrderCriterionLinkId"), \
            when((orderCriterionLink_df.Id.isNull()) & ((criterion_df.Code == '301') | (criterion_df.Code == '303') | (criterion_df.Code == '304')),\
                col("pos.TargetConsumerUnitQuantity")) \
            .otherwise(col("pos.ActualConsumerUnitQuantity")) \
            .alias("ConsumerUnitQuantity"))
    
    
reportPpQuantities_df \
    .where("CollectiveOrderId == 4572634") \
    .orderBy("PositionId") \
    .show(300, False)
    
reportPpQuantities_df.registerTempTable("ReportPpQuantities")

    
    

