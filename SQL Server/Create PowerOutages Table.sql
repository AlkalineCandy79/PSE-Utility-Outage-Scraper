USE [WebGIS]
GO

/****** Object:  Table [SAFETY].[PowerOutages]    Script Date: 1/7/2020 2:50:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [SAFETY].[PowerOutages](
	[ID] [varchar](10) NOT NULL,
	[Title] [varchar](20) NULL,
	[Map_Type] [varchar](10) NULL,
	[Pin_Type] [varchar](10) NULL,
	[Planned] [varchar](3) NULL,
	[Start] [varchar](15) NULL,
	[EstRestore] [varchar](30) NULL,
	[Impact] [numeric](11, 0) NOT NULL,
	[Cause] [varchar](255) NULL,
	[Status] [varchar](255) NULL,
	[Updated] [varchar](15) NULL,
	[Longitude] [decimal](11, 8) NULL,
	[Latitude] [decimal](10, 8) NULL,
	[SysCreateDate] [datetime] NOT NULL,
	[SysChangeDate] [datetime] NOT NULL,
	[Shape] [geometry] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [SAFETY].[PowerOutages] ADD  DEFAULT ((0)) FOR [Impact]
GO

ALTER TABLE [SAFETY].[PowerOutages] ADD  DEFAULT (getdate()) FOR [SysCreateDate]
GO

ALTER TABLE [SAFETY].[PowerOutages] ADD  DEFAULT (getdate()) FOR [SysChangeDate]
GO


