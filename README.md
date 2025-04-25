# UKG Middleware script. Goal is to read timesheet data, craft acceptable timesheet format and push to CMiC timesheets

## Next steps:
  1. Build up fully joined employee record using helper tables (Org Level 1, 2, Project, etc. **DONE**
  2. Build up fully joined timesheet records using employee record and helper tables **DONE**
  3.prove-out data clean up is complete.
  4. Map  job codes structurally **DONE**
  5. Map cost codes via lookup if necessary **DONE**

## CMiC side next steps:
  successfully hit get request against timesheet endpoint **DONE**
  map UKG record against this endpoint **DONE**
  post a timesheet for an existing employee **DONE**

## All around:
  Develop exception logging for unknown cost codes and job codes
  contemplate how to update employee records
1. CMiC employee record case for error msg
2. costing. need business logic on moving from hours to dollars
3. provide charge rate by employee template file. include ee id, job title, cost code, dept, and current actual charge rate
4. ensure we're grabbing worked vs PTO