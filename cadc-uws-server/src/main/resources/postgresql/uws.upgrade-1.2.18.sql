-- rename indices to make rollover maintenance cleaner

ALTER INDEX <schema>.jobid_fkey RENAME TO jobdetail_fkey_<tag>;
ALTER INDEX <schema>.<schema>_jobIndex_creationTime RENAME TO job_creationTime_<tag>;
ALTER INDEX <schema>.<schema>_jobIndex_ownerID RENAME TO job_ownerID_<tag>;