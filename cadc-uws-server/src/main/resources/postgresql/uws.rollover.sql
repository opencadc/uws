-- rollover Job and JobDetail tables to keep active table small and preserve history

-- rename indices
ALTER INDEX <schema>.jobdetail_fkey RENAME TO jobdetail_fkey_<tag>;
ALTER INDEX <schema>.job_creationtime RENAME TO job_creationtime_<tag>;
ALTER INDEX <schema>.job_ownerid RENAME TO job_ownerid_<tag>;
ALTER INDEX <schema>.job_pkey RENAME TO job_pkey_<tag>;

-- rename tables
alter table <schema>.Job rename to Job_<tag>;
alter table <schema>.JobDetail rename to JobDetail_<tag>;

-- the FK constraint in JobDetail->Job automatically follows the Job table rename

