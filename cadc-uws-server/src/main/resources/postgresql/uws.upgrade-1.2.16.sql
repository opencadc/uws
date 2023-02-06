
-- increase size of ownerID for long X509 DN values
alter table <schema>.Job alter column ownerID set data type varchar(128);

