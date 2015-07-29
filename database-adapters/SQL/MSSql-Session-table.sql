CREATE TABLE [dbo].[sessions](
    [sid] [varchar](255) NOT NULL PRIMARY KEY,
    [session] [varchar](max) NOT NULL,
    [expires] [datetime] NOT NULL
)