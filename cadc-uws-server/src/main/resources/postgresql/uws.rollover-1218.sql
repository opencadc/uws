-- rollover Job and JobDetail tables to keep active table small and preserve history
-- this is used by vospace services when upgarding to 1.3.0 with modified jobInfo storage
-- so it is tagged with something like the previous db versiona

-- rename indices
ALTER INDEX <schema>.jobdetail_fkey RENAME TO jobdetail_fkey_1218;
ALTER INDEX <schema>.job_creationtime RENAME TO job_creationtime_1218;
ALTER INDEX <schema>.job_ownerid RENAME TO job_ownerid_1218;
ALTER INDEX <schema>.job_pkey RENAME TO job_pkey_1218;

-- rename tables
alter table <schema>.Job rename to Job_1218;
alter table <schema>.JobDetail rename to JobDetail_1218;

-- the FK constraint in JobDetail->Job automatically follows the Job table rename
