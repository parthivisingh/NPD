[DocumentNo]	SO/24-25/1220
,[CustomerCode]	CU0016
,[OrderDate]	2024-07-04 00:00:00
,[SalespersonCode]	JAYASREE
,[LineNo]	20000
,[MPCODE]	30160
,[DocumentDescription]	Sales - NRSC Charges
,[MFGMode]	PRODUCTION
,[Type]	Order
,[Amount]	10000
,[BacklogAmount]	0
,[Customer_Name]	Bharat Electronics Ltd  - Bangalore
,[Grade]	JSS
,[PlannedQuarter]	Q1 (Apr-Jun)
,[OrderQuarter]	Q2
,[PlannedDeliveryFY]	2024-25
,[OrderFY]	2024-25
,[PlannedMonth]	April
,[MonthName]	July
,[document_Month_Number]	1
,[Order_Month_Number]	4
,[Item]	30160 - Sales - NR
,[Planned_Fy_Flag]	1
,[Ord_Fy_Flag]	1
,[PlannedDeliveryMonthflag]	0
,[OrderMonthflag]	0
,[Quantity]	1
,[OutstandingQuantity]	0
,[QuantityInvoiced]	1
,[PlannedDeliveryDate]	2024-04-07 00:00:00
,[No_of_Lines]	1
,[orderyear]	2024
,[monthyear]	202407
,[MMMMYY]	Jul-24

/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [DocumentNo]
      ,[CustomerCode]
      ,[OrderDate]
      ,[SalespersonCode]
      ,[LineNo]
      ,[MPCODE]
      ,[DocumentDescription]
      ,[MFGMode]
      ,[Type]
      ,[Amount]
      ,[BacklogAmount]
      ,[Customer_Name]
      ,[Grade]
      ,[PlannedQuarter]
      ,[OrderQuarter]
      ,[PlannedDeliveryFY]
      ,[OrderFY]
      ,[PlannedMonth]
      ,[MonthName]
      ,[document_Month_Number]
      ,[Order_Month_Number]
      ,[Item]
      ,[Planned_Fy_Flag]
      ,[Ord_Fy_Flag]
      ,[PlannedDeliveryMonthflag]
      ,[OrderMonthflag]
      ,[Quantity]
      ,[OutstandingQuantity]
      ,[QuantityInvoiced]
      ,[PlannedDeliveryDate]
      ,[No_of_Lines]
      ,[orderyear]
      ,[monthyear]
      ,[MMMMYY]
  FROM [SalesPlanDB].[dbo].[SalesPlanTable]


SO/24-25/1220	CU0016	2024-07-04 00:00:00	JAYASREE	20000	30160	Sales - NRSC Charges	PRODUCTION	Order	10000	0	Bharat Electronics Ltd  - Bangalore	JSS	Q1 (Apr-Jun)	Q2	2024-25	2024-25	April	July	1	4	30160 - Sales - NR	1	1	0	0	1	0	1	2024-04-07 00:00:00	1	2024	202407	Jul-24