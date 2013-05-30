/*
This file demonstrates nesting data in an HBase table

After importing companies.sql, import this file.

Schema mapping: 

java -jar dist/HBaseSQLImport.jar 
	-qn EnabledServices 
	-ty Column 
	-c ServiceName 
	-hbt company 
	-hbcf d 
	-hbq sid_{ServiceId} 
	-hbl ServiceName 
	-hbd "The name one of the company's enabled services" 
	-save
	-hbn

	Should we have a -n Nested parameter?  Or is it enough to specify that the qualifier is based 
	on one of the field values?  Might make it easier to serialize the data.

*/

select c.CompanyId, s.ServiceId, s.[Name]
from dbo.Companies c
join dbo.CompanyServices cs on cs.CompanyId = c.CompanyId
join dbo.Services s on s.ServiceId = cs.ServiceId
join dbo.NotificationSettings ns on ns.NotificationSettingsId = cs.NotificationSettingsId
where c.IsActiveOnThisShard = 1
and c.CompanyStatusTypeId = 1
and c.CompanyTypeId = 1
and ns.Enabled = 1
;
