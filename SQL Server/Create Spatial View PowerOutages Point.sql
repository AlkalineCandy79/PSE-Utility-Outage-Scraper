USE [WebGIS]
GO

/****** Object:  View [SAFETY].[View_Spatial_PowerOutages_Point]    Script Date: 1/7/2020 2:52:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE view 
[SAFETY].[View_Spatial_PowerOutages_Point]

as

select 
	convert (int, ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as ObjectID
	  ,PO.[ID]
      ,PO.[Title]
      ,PO.[Map_Type]
      ,PO.[Pin_Type]
      ,PO.[Planned]
      ,PO.[Start]
      ,PO.[EstRestore]
      ,PO.[Impact]
      ,PO.[Cause]
      ,PO.[Status]
      ,PO.[Updated]
      ,PO.[Longitude]
      ,PO.[Latitude]
      ,PO.[SysCreateDate]
      ,PO.[SysChangeDate]
	  ,PO.[Shape]
	  from [SAFETY].[PowerOutages] as PO
	  where PO.Status <> 'Complete'
GO


