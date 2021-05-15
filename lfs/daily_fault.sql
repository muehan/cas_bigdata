SELECT
	DISTINCT sup.[No] AS SupplierNo,
	ord.DeliveryDateTime AS DeliveryDateTime,
	col.Id AS CollectiveOrderId,
	col.No AS CollectiveOrderNo,
	art.No AS ArticleNo,
	art.Caption AS ArticleCaption,
	crit.Code AS CriterionCode,
	critText.Caption AS CriterionCaption,
	case
		when art.No is null then null
		else (
			select
				SUM(ActualTradeUnitQuantity)
			from
				Position p
				join [order] o on (o.Id = p.OrderId)
			where
				o.CollectiveOrderId = col.Id
				and p.ArticleId = pos.ArticleId
		)
	end as ActualTradeUnitQuantity,
	case
		when art.No is null then null
		else (
			select
				SUM(TargetTradeUnitQuantity)
			from
				Position p
				join [order] o on (o.Id = p.OrderId)
			where
				o.CollectiveOrderId = col.Id
				and p.ArticleId = pos.ArticleId
		)
	end as TargetTradeUnitQuantity,
	ocl.ActualQuantity AS ActualConsumerUnitQuantity,
	ocl.TargetQuantity AS TargetConsumerUnitQuantity,
	ocl.ConsumerUnit,
	ocl.CriterionDeduction,
	ocl.EffectiveDeduction,
	ocl.Performance,
	ocl.TenantId,
	ocl.ReductionFactor,
	ocl.ModifiedBy,
	ocl.CreatedBy,
	CASE
		WHEN ocl.ModifiedBy IS NOT NULL
		AND LEFT(ocl.ModifiedBy, 4) <> 'GMLU' THEN ocl.ModifiedBy
		ELSE CASE
			WHEN LEFT(ocl.CreatedBy, 4) <> 'GMLU' THEN ocl.CreatedBy
		END
	END AS OclUser,
	pt.Code AS PerformanceTypeCode,
	ptx.Caption AS PerformanceTypeCaption,
	cgtx.Caption AS CriterionGroupCaption,
	CASE
		WHEN pt.Code = 'LP' THEN col.CommentLp
		ELSE CASE
			WHEN pt.Code = 'PP' THEN pos.CommentPp
			ELSE pos.CommentMp
		END
	END AS Comment,
	CASE
		WHEN pt.Code = 'LP' THEN null
		ELSE pos.SubcontractorName
	END AS SubcontractorName,
	CASE
		WHEN pt.Code = 'LP' THEN null
		ELSE pos.SubcontractorGgn
	END AS SubcontractorGgn
FROM
	dbo.GetSuppliers(@TenantIds, @SupplierNos, @SupplierGlobalNos) sup
	INNER JOIN dbo.[Order] ord ON ord.SupplierId = sup.Id
	INNER JOIN dbo.Position pos ON pos.OrderId = ord.Id
	INNER JOIN dbo.Assortment assPos ON assPos.Id = pos.AssortmentId
	INNER JOIN dbo.CollectiveOrder col ON col.Id = ord.CollectiveOrderId
	INNER JOIN dbo.OrderCriterionLink ocl ON ocl.CollectiveOrderId = col.Id
	INNER JOIN dbo.PerformanceType pt ON pt.Id = ocl.PerformanceTypeId
	INNER JOIN dbo.PerformanceTypeText ptx ON ptx.PerformanceTypeId = pt.Id
	LEFT JOIN dbo.Article art ON art.Id = ocl.ArticleId
	LEFT JOIN dbo.Assortment assOcl ON assOcl.Id = art.AssortmentId
	INNER JOIN dbo.CriterionAssortmentLink cal ON cal.Id = ocl.CriterionAssortmentLinkId
	INNER JOIN dbo.Criterion crit ON crit.Id = cal.CriterionId
	INNER JOIN dbo.CriterionText critText ON critText.CriterionId = crit.Id
	INNER JOIN dbo.CriterionGroupLink cgl ON cgl.CriterionAssortmentLinkId = cal.Id
	INNER JOIN dbo.CriterionGroup cg ON cg.Id = cgl.CriterionGroupId
	INNER JOIN dbo.CriterionGroupText cgtx ON cgtx.CriterionGroupId = cg.Id
WHERE
	ord.TenantId IN (
		SELECT
			CAST(Id AS SMALLINT)
		from
			dbo.Split(@TenantIds)
	)
	AND ord.DeliveryDateTime >= CAST(@DateAt AS DATE)
	AND ord.DeliveryDateTime < DATEADD(DAY, 1, CAST(@DateAt AS DATE))
	AND ISNULL(art.Id, pos.ArticleId) = pos.ArticleId
	AND (
		@AssortmentCode IS NULL
		OR assPos.Code = @AssortmentCode
	)
	AND (
		@AssortmentCode IS NULL
		OR ISNULL(assOcl.Code, @AssortmentCode) = @AssortmentCode
	)
	AND ISNULL(@PerformanceTypeCode, pt.Code) = pt.Code
	AND critText.LanguageId = @LanguageId
	AND ptx.LanguageId = @LanguageId
	AND cgtx.LanguageId = @LanguageId