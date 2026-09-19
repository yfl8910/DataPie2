### DataPie简介

DataPie是一个通用的数据库导入，导出，数据查询，存储过程调用的客户端工具，基于.NET 10 Winform，支持SQL Server，SQLITE数据库。

-  支持EXCEL,CSV,SQLITE等格式文件的数据库导入导出；

-  支持单表，多表数据导出，支持自定义SQL代码查询数据，导出数据；

-  支持无参数存储过程的调用；

-  支持读取多数据库Schema（SQL Server，SQLITE）；

DataPie is a general purpose database import, export, data query, stored procedure call client tool based on.NET 10 Winform, support SQL Server and SQLITE databases.

-  Support import and export of database files in EXCEL,CSV,SQLITE and other formats;

-  Support single table, multi-table data export, support custom SQL code query data, export data;
 
-  Supports calls of stored procedures without arguments;

-  Supports Database schema read（SQL Server，SQLITE）;
 
-  Export all data from SQL Server to SQLITE database.




---
### SQLite 存储过程脚本

SQL Server 导出到 SQLite 时，会在数据库文件同目录生成 `StoredProcedures/过程名.txt`。
文件包含参数元数据和可在 SQLite 执行的 SQL 正文，不包含 `CREATE PROCEDURE`。
同名过程使用 `schema.过程名.txt`；文件名中的非法字符会被替换并附加唯一标识。

- 支持简单的 `SELECT`、`INSERT`、`UPDATE`、`DELETE`；可以拆分省略分号的连续语句，保留 `INSERT ... SELECT`、子查询和联合查询的结构。复杂结构仍建议显式使用分号。
- 支持输入参数和字面量默认值，以及已有的 `N'...'`、顶层常量 `TOP`、表名 schema 转换。
- 支持 `INSERT ... SELECT TOP n` / `TOP (n)`，将限行应用于排序后的查询结果；`PERCENT`、`WITH TIES` 和带 TOP 的联合查询仍需人工转换。
- 支持单表 `PIVOT(SUM(value) FOR key IN (...))` 后按维度分组并使用 `SUM(ISNULL(pivotColumn, 0))` 求和的形式，转换为条件聚合。复杂透视、透视结果过滤、其他聚合方式不会自动转换。
- 将函数调用 `ISNULL(value, fallback)` 转为 SQLite `IFNULL(value, fallback)`，支持嵌套调用及 `INSERT ... SELECT ... UNION ALL` 中的聚合空值处理。SQLite 的返回值类型规则不同于 SQL Server；依赖隐式类型转换或字符串截断的表达式仍需人工检查。
- 支持 `LEFT`、`LEN`、三参数 `SUBSTRING` 及 `YEAR/MONTH/DAY` 日期字段调用；`LEN` 不计尾部普通空格。支持嵌套调用和 `YEAR(date) * 100 + MONTH(date)`。日期须为 SQLite 可识别的格式（例如 ISO 日期文本）。负截取长度或无法识别的非空日期会触发 SQLite `integer overflow` 保护错误，避免静默改变数据；日期解析与 SQL Server 的全部隐式转换规则并不等价。
- 支持过程开头的 `DECLARE` 局部变量：常量、`NULL`、`GETDATE()`、`YEAR/MONTH/DAY(GETDATE())`。日期本身限 `datetime/datetime2`，年月日限 `int/bigint`；声明可省略分号。
- 日期局部变量在每次调用时按运行程序机器的本地时间统一计算，不会固定为导出日期，也不能由调用方覆盖。源服务器时区不同时需确认业务时间；当前不自动继承源服务器时区。
- 声明初始化支持年月日函数、数值常量或已声明的同类型数值变量，加减整数常量，例如 `DECLARE @month int = MONTH(GETDATE()) - 1`、`DECLARE @next int = @month + 1`。按声明顺序计算，再执行后续 IF 赋值；支持连续加减和 NULL 传递，不支持向后引用、乘除或括号表达式。
- 支持开头局部变量声明之后、数据库语句之前连续出现的 `IF @day < 15 SET @month = @month - 1`。条件支持整数变量/常量及 `= <> != < <= > >=`；赋值目标须为整数局部变量，表达式支持变量、整数常量和加减。每次调用按原顺序计算，再绑定 SQL 参数；条件为 NULL 时不赋值。当前不支持 ELSE、嵌套条件、循环、括号表达式或执行 SQL 后再赋值，不会自动更改跨年业务规则。
- 支持加号两侧为数值常量或已知数值变量的运算，例如 `@year * 100 + @month`；无法确认类型的加号仍需人工转换，不能直接当成 SQLite 字符串拼接。
- 不支持的变量声明、赋值及依赖这些变量的语句会注释掉；独立 SQL 继续保留。输出参数不可用，但不引用它的 SQL 仍可导出。仅被跳过语句引用的参数不再要求调用方提供。
- 不支持的完整语句（如含 `+`、`COLLATE` 或未通过 SQLite 语法检查）会以 `UNSUPPORTED` 注释保留，其他可执行语句继续导出，脚本标记为 `PARTIAL`。文件中出现 `UNSUPPORTED` 注释不代表整个文件禁止调用。不会单独删除过滤条件或表达式。
- 遇到无法安全拆分的控制流时，保守地注释该语句及后续内容，避免丢失条件。无法解析过程声明或参数结构，或没有任何可执行语句时，仍禁止调用。多条语句请使用分号分隔。
- 部分转换不保证与原过程业务语义等价，尤其是后续语句依赖被跳过的写入时，请检查导出注释和迁移警告。更新转换器后需重新导出脚本。
- SQLite 本身不提供存储过程；DataPie 的 `RunProcedure` 接口读取这些文本并绑定参数。写操作在一个事务中执行。
- SQLite 的过程列表包含完整及部分转换的可执行脚本，`ExecuteProceduresAsync` 可直接调用；部分执行后会显示跳过内容的警告。界面可运行无必填参数的脚本；带必填参数的脚本由调用代码传参。
- 请将数据库、整个 `StoredProcedures` 文件夹一起移动，保持数据库文件名不变。首行元数据和 `.datapie-*.json` 索引用于识别参数及所属数据库。

```csharp
using var db = DbAccessFactory.Create("Data Source=export.db", "SQLITE");

// 参数值由数据库驱动绑定，不拼接到 SQL 中。
using var rows = db.RunProcedure("GetItems",
    new IDataParameter[] { db.CreatePara("@code", "EUR") }, "Items");

db.RunProcedure("UpdateItem",
    new IDataParameter[] { db.CreatePara("@id", 1), db.CreatePara("@code", "CH") },
    out int affectedRows);
```

### 软件界面

-  数据库登陆（点击Test,选择相应的SQL server数据库）

![image](https://user-images.githubusercontent.com/2750715/143333355-417c79bf-19a4-45b0-a7fe-7e4f4b7af359.png)

-  数据导入（支持单个文件及文件夹中文件批量导入）

![image](https://user-images.githubusercontent.com/2750715/143333471-266df7df-e990-4250-b7b4-adbd4d6a359a.png)

-  数据导出（支持多表和多试图批量导出到同一个EXCEL）

![image](https://user-images.githubusercontent.com/2750715/143333525-059847d0-c590-4659-a3b8-f713185b0d44.png)

-  SQL查询

![image](https://user-images.githubusercontent.com/2750715/143333585-c0f24d49-f8d7-4f01-a611-e41494409023.png)

-  SQL Server导出表到SQLITE

![image](https://user-images.githubusercontent.com/2750715/143333691-295c5853-3682-4447-97c3-845d94dcf44d.png)









