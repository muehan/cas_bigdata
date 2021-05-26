SELECT
	  col.Id AS CollectiveOrderId
	, ord.Id AS OrderId
	, ord.SupplierId
	, pos.Id AS PositionId
	, pos.ArticleId
	, pos.AssortmentId
	, ocl.Id AS OrderCriterionLinkId
	, CASE
		WHEN ocl.Id IS NOT NULL AND crit.Code IN ('301', '303', '304') THEN
			pos.TargetConsumerUnitQuantity
        ELSE
			pos.ActualConsumerUnitQuantity
      END AS ConsumerUnitQuantity
FROM Position pos
JOIN [Order] ord on ord.Id = pos.OrderId
	JOIN CollectiveOrder col on col.Id = ord.CollectiveOrderId
		LEFT OUTER JOIN OrderCriterionLink ocl on ocl.CollectiveOrderId = col.Id and ocl.ArticleId = pos.ArticleId
			LEFT OUTER JOIN PerformanceType pt ON pt.Id = ocl.PerformanceTypeId
			LEFT OUTER JOIN CriterionAssortmentLink cal on cal.Id = ocl.CriterionAssortmentLinkId
				LEFT OUTER JOIN Criterion crit on crit.id = cal.CriterionId
				LEFT OUTER JOIN CriterionGroupLink cgl on cgl.CriterionAssortmentLinkId = cal.Id
					LEFT OUTER JOIN CriterionGroup cg ON cg.Id = cgl.CriterionGroupId
WHERE
	(pt.Id is null or pt.Code = 'PP')
	AND (ocl.Id IS NULL OR cg.Code = '10') --10 = Status Criterion