namespace JsonTools
{
    static class SqlStrings
    {
        public const string buildTempTable = 
@"drop table if exists [#{tableName}];
select * into [#{tableName}]
from [{tableName}]
where 1=0";

        public const string mergeTempTableStart =
@"declare @TBN varchar(128) = '{tableName}';
declare @RESULT nvarchar(max);
declare @onTB TABLE(oCol varchar(128));
{mergeOnTB}
declare @fieldTB TABLE(fCol varchar(128));
{insertFieldTB}

with COLSORT as (
 select oCol, fCol, COLUMN_NAME,
  COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') idColumn,
  (case when first_value(fCol) over(order by fcol desc) is null then 0 else 1 end) specified
 from INFORMATION_SCHEMA.COLUMNS
  left outer join @onTB on COLUMN_NAME=oCol
  left outer join @fieldTB on COLUMN_NAME=fCol
 where TABLE_NAME= @TBN),
COLSPEC as (
 select oCol onCol, 
  case when specified=1 then fCol
       when specified=0 and oCol is null then COLUMN_NAME
       else null end updateCol,
  case when specified=1 and idColumn=1 then fCol
       when idColumn=0 then COLUMN_NAME
	   else null end insertCol
 from COLSORT a)
select @RESULT='merge ['+@TBN+'] t using [#'+@TBN+'] s on '+
 string_agg('t.['+onCol+']=s.['+onCol+']', 'and ')+
 ' when matched and ('+
 string_agg('t.['+updateCol+']<>s.['+updateCol+']', 'and ')+
 ') then update set '+
 string_agg('t.['+updateCol+']=s.['+updateCol+']', ',')+
 ' when not matched by target then insert('+
 string_agg('['+insertCol+']',',')+') values('+
 string_agg('s.['+insertCol+']',',')+";

        public const string mergeTempTablePurge =
@"')when not matched by source then delete;'";

        public const string mergeTempTableNoPurge =
@"');'";

        public const string mergeTempTableEnd =
@"from COLSPEC;
EXEC sp_executesql @RESULT
";
    }
}