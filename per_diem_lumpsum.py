import pandas as pd
import pay_period
from sqlalchemy.orm import Session
from sqlalchemy import select, create_engine
from models import CMiC_Timesheet_Entry, CMiC_Employee

BURDEN_EARNING_CODES = {"PERDM", "LIVAL"}
BURDEN_COST_CODE = "01731"
BURDEN_COST_CATEGORY = "L"
CREDIT_ACCOUNT = "600.004"
COMPANY_CODE = "HJR"
def _get_job_allocations(session: Session, emp_no: str, pay_group: str, ppr_year: int, ppr_period: int) -> dict[str, float]:
    """
    Returns {job_code: proportion} for a given employee in a given pay period.
    Proportion is based on TshNormalHours across jobs.
    Excludes overhead (TshTypeCode == 'G').
    """
    stmt = (
        select(CMiC_Timesheet_Entry)
        .where(CMiC_Timesheet_Entry.TshEmpNo == emp_no)
        .where(CMiC_Timesheet_Entry.TshPprYear == ppr_year)
        .where(CMiC_Timesheet_Entry.TshPprPeriod == ppr_period)
        .where(CMiC_Timesheet_Entry.TshTypeCode == 'J')
    )
    entries = session.scalars(stmt).all()

    if not entries:
        return {}

    total_hours = sum(e.TshNormalHours for e in entries)
    if total_hours == 0:
        return {}

    allocations: dict[str, float] = {}
    for entry in entries:
        job = entry.TshJobdeptwoId
        allocations[job] = allocations.get(job, 0) + entry.TshNormalHours

    return {job: hours / total_hours for job, hours in allocations.items()}


def build_journal_entries(
    earnings_df: pd.DataFrame,
    session: Session,
    pay_period_end_date: str,
) -> list[dict]:
    """
    Produces a list of journal entry row dicts for PERDM and LIVAL earnings.
    Each burden amount is split proportionally across jobs worked in the period.
    """
    burden_rows = earnings_df[
        (earnings_df["earningCode"].isin(BURDEN_EARNING_CODES)) &
        (earnings_df["companyId"] == "PQCD4")
    ].copy()

    if burden_rows.empty:
        raise ValueError(f"No PERDM/LIVAL rows found for period {pay_period_end_date}")

    journal_lines = []
    pay_periods_df = pd.read_csv('./DataFiles/Pay_Period.csv', skiprows=2)
    pay_period_df = pay_period.find_period_by_date(pay_periods_df,'B',pay_period_end_date)
    enddate = pay_period_df['End Date'].iloc[0]
    CMiC_PAY_PERIOD = enddate 
    for _, row in burden_rows.iterrows():
        emp_no = f"{row["employeeNumber"]:06}" #treat emp no as six digit
        amount = round(row["currentAmount"], 2)
        earning_code = row["earningCode"]
        pay_group = row["payGroup"]

        # Determine pay period year/period from CMiC_Timesheet_Entry
        # Use a sample entry to get ppr_year and ppr_period for this employee
        sample = session.scalars(
            select(CMiC_Timesheet_Entry)
            .where(CMiC_Timesheet_Entry.TshEmpNo == emp_no)
        ).first()

        if sample is None:
            raise ValueError(
                f"No CMiC_Timesheet_Entry records found for employee {emp_no} "
                f"— cannot determine pay period for {earning_code} journal entry."
            )

        ppr_year = sample.TshPprYear
        ppr_period = sample.TshPprPeriod
        allocations = _get_job_allocations(session, emp_no, pay_group, ppr_year, ppr_period)

        if not allocations:
            raise ValueError(
                f"Employee {emp_no} has {earning_code} earnings but no job-costed "
                f"timesheet entries in period {ppr_year}/{ppr_period}. Cannot allocate burden."
            )
        cmic_emp = session.scalars(
            select(CMiC_Employee)
            .where(CMiC_Employee.EmpNo == emp_no)
        ).first()

        if cmic_emp is None:
            raise ValueError(
                f"No CMiC_Employee record found for employee {emp_no} "
                f"— cannot determine home department for {earning_code} journal entry."
            )

        home_dept = cmic_emp.EmpHomeDeptCode
        for job_code, proportion in allocations.items():
            allocated_amount = round(amount * proportion, 2)

            # Debit: job cost
            journal_lines.append({
                "type": "J",
                "company": COMPANY_CODE,
                "job_dept": job_code,
                "cost_code": BURDEN_COST_CODE,
                "cost_cat": BURDEN_COST_CATEGORY,
                "crew_code":"",
                "quantity":"",
                "rate":"",
                "weighted_measure":"LS",
                "debit": allocated_amount,
                "credit": "",
                "source_code": emp_no,
                "source_description": f"{cmic_emp.EmpLastName}, {cmic_emp.EmpFirstName}",
                "ref_code": sample.TshPrnCode,
                "ref_description": f"{sample.TshPrnCode}{ppr_year}{ppr_period}",
            })

            # Credit: clearing account
            journal_lines.append({
                "type": "G",
                "company": COMPANY_CODE,
                "job_dept": home_dept,
                "cost_code": CREDIT_ACCOUNT,
                "cost_cat": "",
                "crew_code":"",
                "quantity":"",
                "rate":"",
                "weighted_measure":"",
                "debit": "",
                "credit": allocated_amount,
                "source_code": emp_no,
                "source_description": f"{cmic_emp.EmpLastName}, {cmic_emp.EmpFirstName}",
                "ref_code": sample.TshPrnCode,
                "ref_description": f"{sample.TshPrnCode}{ppr_year}{ppr_period}",
            })

    return journal_lines


def export_journal_entries(
    earnings_df: pd.DataFrame,
    session: Session,
    pay_period_end_date: str,
    output_dir: str = "./DataFiles"
) -> dict[str, int]:
    lines = build_journal_entries(earnings_df, session, pay_period_end_date)
    filepath = f"{output_dir}/journal_entries_{pay_period_end_date}.csv"
    result_df = pd.DataFrame(lines)
    result_df = result_df.sort_values(by=["type","source_code"],ascending=False)
    result_df.to_csv(filepath, index=False)
    return {filepath: len(lines)}
if __name__ == "__main__":
    earnings_df = pd.read_csv("./DataFiles/ukg_earnings_history.csv")
    engine = create_engine("sqlite:///DataFiles/utm.db", echo=False)
    session = Session(engine)
    pay_period_end_date = "20260501"
    output_dir = "./DataFiles"
    export_journal_entries(earnings_df, 
        session,
        pay_period_end_date
        )