-- rename indices to make rollover maintenance cleaner

ALTER INDEX <schema>.jobid_fkey RENAME TO jobdetail_fkey;
ALTER INDEX <schema>.<schema>_jobIndex_creationTime RENAME TO job_creationTime;
ALTER INDEX <schema>.<schema>_jobIndex_ownerID RENAME TO job_ownerID;