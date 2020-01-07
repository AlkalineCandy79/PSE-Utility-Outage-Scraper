USE [WebGIS]
GO

/****** Object:  Table [SAFETY].[PowerOutages_Extent]    Script Date: 1/7/2020 2:50:55 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [SAFETY].[PowerOutages_Extent](
	[ID] [varchar](10) NOT NULL,
	[SysCreateDate] [datetime] NOT NULL,
	[SysChangeDate] [datetime] NOT NULL,
	[Shape] [geometry] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [SAFETY].[PowerOutages_Extent] ADD  DEFAULT (getdate()) FOR [SysCreateDate]
GO

ALTER TABLE [SAFETY].[PowerOutages_Extent] ADD  DEFAULT (getdate()) FOR [SysChangeDate]
GO


