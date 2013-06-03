select 
    c.name as [Name], 
    ty.name as [Type], 
    case    when ty.name in ('varchar', 'nvarchar') 
            then case when c.max_length = -1 
                            then 'max' 
                            else cast(c.max_length as varchar(10))  
                            end
            when ty.name = 'numeric' 
            then cast(c.precision as varchar(2)) + 
                    ',' + cast(c.scale as varchar(2))
            else ''
            end as [Length], 
    case when c.is_nullable = 1 then 'null' else 'not null' end as Nullable, 
    cast(case when exists (
        select 1 from sys.indexes ix
        join sys.index_columns ic on ic.object_id = t.object_id and ic.index_id = ix.index_id
             and c.column_id = ic.column_id
        where ix.object_id = t.object_id
        and ix.is_primary_key = 1
        ) then 1 else 0 end as bit) as IsPrimaryKey
from sys.tables t
join sys.schemas s on s.schema_id = t.schema_id
join sys.columns c on c.object_id = t.object_id
join sys.types ty on ty.user_type_id = c.user_type_id
where s.name = ? -- 1
and t.name = ? -- 2
order by c.column_id
;

