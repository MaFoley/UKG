# UKG Middleware script. Goal is to read timesheet data, craft acceptable timesheet format and push to CMiC timesheets

## Next steps:
  1. Build up fully joined employee record using helper tables (Org Level 1, 2, Project, etc. **DONE**
  2. Build up fully joined timesheet records using employee record and helper tables **DONE**
  3. Prove-out data clean up is complete.
  4. Map job codes structurally
  5. Map cost codes via lookup if necessary

## CMiC side next steps:
  successfully hit get request against timesheet endpoint
  map UKG record against this endpoint
  post a timesheet for an existing employee

## All around:
  Develop exception logging for unknown cost codes and job codes
  contemplate how to update employee records
