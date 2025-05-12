import pytest
from models import Employee, CMiC_Employee, Time, CMiC_Timesheet_Entry
import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime
@pytest.fixture
def get_engine():
    return sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
@pytest.fixture
def get_sqla_session(get_engine):
    return Session(get_engine)
@pytest.fixture
def emp_stmt():
    return sqlalchemy.select(Employee).where(Employee.PaygroupId == 18)

def test_Employee(get_sqla_session, emp_stmt):
    employee = get_sqla_session.execute(emp_stmt).scalars().first()
    assert employee.__class__ == Employee
    assert len(employee.shortEmpId()) == 6
    assert employee.companyCode() == 'PQCD4'

def test_CMiC_Employee(get_sqla_session, emp_stmt):
    ukg_employees = get_sqla_session.execute(emp_stmt).scalars()
    for ukg_employee in ukg_employees:
        cmic_employee = CMiC_Employee(ukg_employee)
        cmic_employee1 = CMiC_Employee(ukg_employee)
        assert cmic_employee.EmpNo == ukg_employee.shortEmpId()
        assert cmic_employee1 is not cmic_employee
        assert cmic_employee == cmic_employee1

@pytest.fixture
def time_stmt():
    return sqlalchemy.select(Time)

@pytest.mark.parametrize(('date', 'expected_TshPprPeriod'),
                         [(datetime(2025,1,1),1)
                          ,(datetime(2025,12,27),26)
                          ,(datetime(2025,4,6),8)
                          ,(datetime(2025,1,25),2)
                          ])
def test_CMiC_Timesheet_Entry_PprPeriod(date, expected_TshPprPeriod, get_sqla_session, time_stmt):
    time = get_sqla_session.execute(time_stmt).scalars().first()
    cmic_timesheet_entry = CMiC_Timesheet_Entry(time)
    assert cmic_timesheet_entry._TshPprPeriod(date)==expected_TshPprPeriod

def test_CMiC_Timesheet_Entry(get_sqla_session, time_stmt):
    time = get_sqla_session.execute(time_stmt).scalars().first()
    cmic_timesheet_entry = CMiC_Timesheet_Entry(time)
    copy_cmic_timesheet_entry = CMiC_Timesheet_Entry(time)
    assert cmic_timesheet_entry is not copy_cmic_timesheet_entry
    assert cmic_timesheet_entry == copy_cmic_timesheet_entry