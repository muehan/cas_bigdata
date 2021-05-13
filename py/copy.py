import shutil

from_file = open('D:\downloads\position.csv','r')
to_file = open('D:\downloads\position3.csv','w')

from_file.readline() # and discard
to_file.write('Id;TenantId;AssortmentId;OrderId;ArticleId;CreatedAt;CreatedBy;ModifiedAt;TargetTradeUnitQuantity;TradeUnit;TargetConsumerUnitQuantity;ConsumerUnit;ActualTradeUnitQuantity;ActualConsumerUnitQuantity;OrderPositionNo;HasDeliveries;IsEvaluated;DeliveryWithoutOrder;CommentMp;CommentPp;QuantitiesModifiedByUser;SubcontractorGgn;SubcontractorName')
shutil.copyfileobj(from_file, to_file)

from_file.close()
to_file.close()