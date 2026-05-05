from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from types import SimpleNamespace
from datetime import datetime, timedelta
from dateutil import parser, tz
from sqlalchemy.orm import relationship
import pandas as pd
import tomllib

with open("secrets.toml", "rb") as f:
    config = tomllib.load(f)


class Base(DeclarativeBase):
    pass

class Job(Base):
    __tablename__ = "Job"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class OrgLevel1(Base):
    __tablename__ = "OrgLevel1"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class OrgLevel2(Base):
    __tablename__ = "OrgLevel2"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    CMiC_Department_ID: Mapped[str] = mapped_column(String(), nullable=True)
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r}, CMiC_Department_ID={self.CMiC_Department_ID!r})"
class OrgLevel3(Base):
    __tablename__ = "OrgLevel3"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class Paygroup(Base):
    __tablename__ = "Paygroup"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def get_cmic_payrun(self):
        if self.name == "HJRC":
            return 'B'
        elif self.name == "HJRU":
            return 'W'
        else:
            return 'Not valid Pay Run'
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class CMiC_Project(Base):
    __tablename__ = "CMiC_Project"
    JobCode: Mapped[str] = mapped_column(primary_key=True)
    JobName: Mapped[str] = mapped_column(String())
    JobDefaultDeptCode: Mapped[str] = mapped_column(String())
    JobCostFlag: Mapped[bool] 
    JobCodeNoDots: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(Id={self.JobCode!r}, name={self.JobName!r}, jobCodeNoDots={self.JobCodeNoDots!r})"
class Project(Base):
    __tablename__ = "Project"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(ForeignKey("CMiC_Project.JobCodeNoDots"))
    description: Mapped[str] = mapped_column(String())
    #cmic_name: Mapped[str] = mapped_column(String())
    cmic_project: Mapped["CMiC_Project"] = relationship()
    def isOverheadProject(self) -> bool:
        return self.name[-4:] == "0000"
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class Location(Base):
    __tablename__ = "Location"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    CMiC_Department_ID: Mapped[str] = mapped_column(String(), nullable=True)
    def __repr__(self):
        repr_str = f"{self.__class__.__name__}"
        repr_str += '('
        
        for key, val in self.__dict__.items():
            val       = f"'{val}'" if isinstance(val, str) else val
            repr_str += f"{key}={val}\n, "
        
        return repr_str.strip(", ") + ')'
class Time(Base):
    __tablename__ = "Time"
    Id: Mapped[int] = mapped_column(primary_key=True)
    EmpId: Mapped[int] = mapped_column(ForeignKey("Employee.EmpId"))
    employee: Mapped["Employee"] = relationship(back_populates="time_entries")
    WorkDate: Mapped[str]
    PaycodeId: Mapped[int] 
    PaycodeExp: Mapped[str]
    InOrg: Mapped[str] = mapped_column(nullable = True)
    OutOrg: Mapped[str]= mapped_column(nullable = True)
    In: Mapped[str] = mapped_column(nullable = True)
    Out: Mapped[str] = mapped_column(nullable = True)
    InExp: Mapped[str] = mapped_column(nullable = True)
    OutExp: Mapped[str] = mapped_column(nullable = True)
    ShiftExp: Mapped[str]= mapped_column(nullable = True)
    RegHr: Mapped[float] 
    SchHrs: Mapped[float] 
    Overt1: Mapped[float] 
    Overt2: Mapped[float] 
    Overt3: Mapped[float] 
    Overt4: Mapped[float] 
    Overt5: Mapped[float] 
    RegPay: Mapped[float] 
    Ot1Pay: Mapped[float] 
    Ot2Pay: Mapped[float] 
    Ot3Pay: Mapped[float] 
    Ot4Pay: Mapped[float] 
    Ot5Pay: Mapped[float] 
    WeekReg: Mapped[float] 
    WeekOt1: Mapped[float] 
    WeekOt2: Mapped[float] 
    WeekOt3: Mapped[float] 
    WeekOt4: Mapped[float] 
    WeekOt5: Mapped[float] 
    ReasonId: Mapped[str] = mapped_column(nullable=True)
    PaygroupId: Mapped[int] = mapped_column(ForeignKey("Paygroup.Id"))
    paygroup: Mapped["Paygroup"] = relationship()
    LocationId: Mapped[int] = mapped_column(ForeignKey("Location.Id"))
    JobId: Mapped[int] = mapped_column(ForeignKey("Job.Id"))
    job: Mapped["Job"] = relationship()
    ProjectId: Mapped[int] = mapped_column(ForeignKey("Project.Id"))
    project: Mapped["Project"] = relationship()
    OrgLevel1Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel1.Id"))
    OrgLevel2Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel2.Id"))
    OrgLevel3Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel3.Id"))
    orglevel3: Mapped["OrgLevel3"] = relationship()
    OrgLevel4Id: Mapped[int]
    PeriodTh: Mapped[float]
    SiteIn: Mapped[str] =mapped_column(nullable = True)
    SiteOut: Mapped[float]=mapped_column(nullable = True)
    QuanGood: Mapped[float]=mapped_column(nullable = True)
    QuanScrap: Mapped[float]=mapped_column(nullable = True)
    BegSched: Mapped[float]=mapped_column(nullable = True)
    EndSched: Mapped[float]=mapped_column(nullable = True)
    Status: Mapped[int] =mapped_column(nullable = True)
    AdjustmentDate: Mapped[str] = mapped_column(nullable=True)
    InRounded: Mapped[str] = mapped_column(nullable = True)
    OutRounded: Mapped[str]= mapped_column(nullable = True)
    def companyCode(self):
        #HJRC or HJRU become HJR
        return self.paygroup.name[:3]
    def __repr__(self) -> str:
        return f"Time Object (Id={self.Id!r}, EmpId={self.EmpId!r})"
class Employee(Base):
    __tablename__ = "Employee"
    EmpId: Mapped[str] = mapped_column(primary_key=True)
    time_entries: Mapped[List["Time"]] = relationship(back_populates="employee")
    Id: Mapped[int]
    CardNum: Mapped[str]
    SupId: Mapped[str]
    SupName: Mapped[str]
    FirstName: Mapped[str]
    LastName: Mapped[str]
    Active: Mapped[str]
    Email: Mapped[str]
    BirthDate: Mapped[str]= mapped_column(nullable = True)
    HireDate: Mapped[str]
    HoliRule: Mapped[str]
    PayCate: Mapped[str]
    PayMethod: Mapped[int]
    PayType: Mapped[int]
    LocationId: Mapped[int] = mapped_column(ForeignKey("Location.Id"))
    location: Mapped["Location"] = relationship()
    JobId: Mapped[int] = mapped_column(ForeignKey("Job.Id")) 
    job: Mapped["Job"] = relationship()#aka CMiC Trade Code
    ProjectId: Mapped[int] = mapped_column(ForeignKey("Project.Id"))
    OrgLevel1Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel1.Id"))
    OrgLevel2Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel2.Id"))
    OrgLevel3Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel3.Id"))
    orglevel3: Mapped["OrgLevel3"] = relationship()
    OrgLevel4Id: Mapped[int]
    PayPolicyId: Mapped[int]
    PaygroupId: Mapped[int] = mapped_column(ForeignKey("Paygroup.Id"))
    paygroup: Mapped["Paygroup"] = relationship()
    ShiftId: Mapped[int]
    AccessGroupId: Mapped[int]
    ChargeRate: Mapped[float] = mapped_column(nullable=True)
    def companyCode(self):
        idParts = self.EmpId.split('-')
        if len(idParts) > 1:
            return idParts[1]
        else:
            return idParts[0]
    def shortEmpId(self)->str:
        """
        this method extracts CMiC short ee code from UKG long code
        CMiC uses 6 digit employee code. UKG uses 9+company code
        """
        return self.EmpId[3:9]
    def __repr__(self) -> str:
        return f"(Employee object Id={self.Id!r}, EmpId={self.EmpId!r}, Name: {self.FirstName} {self.LastName})"
class CMiC_Timesheet_Entry:
    """
    Meat and potatoes of the middleware.
    Constructor ingests a UKG Time object and generates the required format for posting to CMiC
    """
    __tablename__ = "CMiC_Timesheet_Entry"

    Id: Mapped[int] = mapped_column(primary_key=True)# UKG Time.Id for auditing
    TshPrnCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshDate: Mapped[str] = mapped_column(String(), nullable=True)
    TshPprYear: Mapped[int] = mapped_column(nullable=True)
    TshPprPeriod: Mapped[int] = mapped_column(nullable=True)
    TshEmpNo: Mapped[str] = mapped_column(String(), nullable=True)
    TshUnionCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshTradeCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshPhsacctwiId: Mapped[str] = mapped_column(String(), nullable=True)
    TshTypeCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshJobdeptwoId: Mapped[str] = mapped_column(String(), nullable=True)
    TshNormalHours: Mapped[float] = mapped_column(nullable=True)
    TshCompCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshWorkCompCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshCatexpId: Mapped[str] = mapped_column(String(), nullable=True)
    TshOtHours: Mapped[float] = mapped_column(nullable=True)
    TshDotHours: Mapped[float] = mapped_column(nullable=True)
    TshWcbCode: Mapped[str] = mapped_column(String(), nullable=True)
    TshOhType: Mapped[str] = mapped_column(String(), nullable=True)
    TshDocumentNo: Mapped[str] = mapped_column(String(), nullable=True)

    WEEKLYPAYGROUP = 'HJRU'
    BIWEEKLYPAYGROUP = 'HJRC'

    @classmethod
    def from_time_entry(cls, time_entry: "Time") -> "CMiC_Timesheet_Entry":
        obj = cls()
        obj.Id = time_entry.Id
        obj.TshPrnCode = obj._findPayRun(time_entry.paygroup.name)
        obj.TshDate = parser.parse(time_entry.WorkDate).strftime('%Y-%m-%d')
        obj.TshPprYear = obj._TshPprYear(parser.parse(time_entry.WorkDate))
        obj.TshPprPeriod = obj._TshPprPeriod(parser.parse(time_entry.WorkDate))
        obj.TshEmpNo = time_entry.employee.shortEmpId()
        obj.TshUnionCode = None
        if time_entry.job is not None:
            obj.TshTradeCode = time_entry.job.name
            obj.TshPhsacctwiId = time_entry.orglevel3.name
        else:
            obj.TshTradeCode = None
            obj.TshPhsacctwiId = None

        project_name = time_entry.project.name if time_entry.project else ""

        if project_name[-4:] == "0000" or "PROJ" in project_name or "Z" == project_name:
            obj.TshTypeCode = 'G'
            obj.TshJobdeptwoId = time_entry.employee.location.CMiC_Department_ID
            obj.TshPhsacctwiId = '600.004'
        elif time_entry.project and time_entry.project.cmic_project:
            obj.TshTypeCode = 'J'
            obj.TshJobdeptwoId = time_entry.project.cmic_project.JobCode
        else:
            obj.TshJobdeptwoId = None

        obj.TshNormalHours = time_entry.RegHr
        obj.TshCompCode = time_entry.companyCode()
        obj.TshWorkCompCode = obj.TshCompCode
        obj.TshCatexpId = 'L'
        obj.TshOtHours = time_entry.Overt1
        obj.TshDotHours = time_entry.Overt2
        obj.TshWcbCode = None
        obj.TshOhType = None
        obj.TshDocumentNo = "-".join([obj.TshEmpNo, obj.TshPrnCode + str(obj.TshPprYear) + str(obj.TshPprPeriod)])
        return obj
    def _findPayRun(self, paygroup: str) -> str:
        if paygroup.upper().strip() == self.BIWEEKLYPAYGROUP:
            return 'B'
        elif paygroup.upper().strip() == self.WEEKLYPAYGROUP:
            return 'W'
        else:
            return 'New Paygroup'
    def _TshPprYear(self, date: datetime) -> int:
        #if tshdate > prior period start date and <= period end date, then return pay period
        yearEnds = {2025: datetime(2025,12,27)
                    ,2026: datetime(2026,12,26)}
        #TODO: future-proof for non 2026 years
        if date <= yearEnds[2026] and date > yearEnds[2025]:
            return 2026
        else:
            return date.year
    def _TshPprPeriod(self, date: datetime) -> int:
        #if tshdate > prior period start date and <= period end date, then return pay period
        periodEndDate = datetime(2024,12,29)
        #B for biweekly, else Weekly
        periodLength = 14 if self.TshPrnCode == 'B' else 7
        numPeriods = 26 if self.TshPrnCode == 'B' else 52
        delta = date - periodEndDate
        return (delta.days // periodLength  % numPeriods) +1

    def payload_dict(self, excluded_keys:set = {"Id"}) -> dict:
        return {key : value for key, value in self.__dict__.items() if key not in excluded_keys}
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return all([
            self.TshEmpNo == other.TshEmpNo
            ,self.TshDate == other.TshDate
            ,self.TshJobdeptwoId == other.TshJobdeptwoId
            ,self.TshPhsacctwiId == other.TshPhsacctwiId 
        ])      
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        repr_str = f"{self.__class__.__name__}"
        repr_str += '('
        
        for key, val in self.__dict__.items():
            val       = f"'{val}'" if isinstance(val, str) else val
            repr_str += f"{key}={val}\n, "
        
        return repr_str.strip(", ") + ')'
class CMiC_Employee(Base):   
    __tablename__ = "CMiC_Employee"
    EmpNo: Mapped[str] = mapped_column(String(), primary_key=True)
    EmhActionCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpCreateAccessCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmhEffectiveDate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPrimaryEmpNo: Mapped[str] = mapped_column(String(), nullable=True)
    EmpUser: Mapped[str] = mapped_column(String(), nullable=True)
    EmpLastName: Mapped[str] = mapped_column(String(), nullable=True)
    EmpFirstName: Mapped[str] = mapped_column(String(), nullable=True)
    EmpSinNo: Mapped[str] = mapped_column(String(), nullable=True)
    EmpStatus: Mapped[str] = mapped_column(String(), nullable=True)
    EmpDateOfBirth: Mapped[str] = mapped_column(String(), nullable=True)
    EmpHireDate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpCompCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpDeptCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPrnCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPygCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpTrdCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpWrlCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpHourlyRate: Mapped[float] = mapped_column(nullable=True)
    EmpChargeOutRate: Mapped[float] = mapped_column(nullable=True)
    EmpBillingRate: Mapped[float] = mapped_column(nullable=True)
    EmpSecGrpEmpCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpFilingStatus: Mapped[str] = mapped_column(String(), nullable=True)
    EmpVUuid: Mapped[str] = mapped_column(String(), nullable=True)
    EmpCountryCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpStateCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpZipCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpFlsaType: Mapped[str] = mapped_column(String(), nullable=True)
    EmpVertexGeocodeSource: Mapped[str] = mapped_column(String(), nullable=True)
    EmpType: Mapped[str] = mapped_column(String(), nullable=True)
    EmpFullPartTime: Mapped[str] = mapped_column(String(), nullable=True)
    EmpSubStatus: Mapped[str] = mapped_column(String(), nullable=True)
    EmpUnionized: Mapped[str] = mapped_column(String(), nullable=True)
    EmpAdjustedSeriviceDate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpYearWorkingDays: Mapped[float] = mapped_column(nullable=True)
    EmpYearWorkingHours: Mapped[float] = mapped_column(nullable=True)
    EmpUeValidFlag: Mapped[str] = mapped_column(String(), nullable=True)
    EmpHomeDeptCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpGlAccCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPayrollClearAccCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpDrClearAccCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpLevAcruGlAccCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpLevClearAccCode: Mapped[str] = mapped_column(String(), nullable=True)
    EmpAnnualSalary: Mapped[float] = mapped_column(nullable=True)
    EmpPreferPayRate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPreferChargeRate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpPreferBillRate: Mapped[str] = mapped_column(String(), nullable=True)
    EmpDirectDepMethod: Mapped[str] = mapped_column(String(), nullable=True)

    @classmethod
    def from_employee(cls, emp: "Employee", effective_date: str) -> "CMiC_Employee":
        obj = cls()
        obj.EmhActionCode = "NR"
        obj.EmpCreateAccessCode = "N"
        obj.EmhEffectiveDate = effective_date
        obj.EmpNo = emp.shortEmpId()
        obj.EmpPrimaryEmpNo = emp.shortEmpId()
        obj.EmpUser = 'NSMITH'
        obj.EmpLastName = emp.LastName
        obj.EmpFirstName = emp.FirstName
        obj.EmpSinNo = '123456789'
        obj.EmpStatus = 'A'
        obj.EmpDateOfBirth = '1999-01-01'
        obj.EmpHireDate = '1999-01-02'
        obj.EmpCompCode = 'HJR'
        obj.EmpDeptCode = emp.location.CMiC_Department_ID
        obj.EmpPrnCode = emp.paygroup.get_cmic_payrun()
        obj.EmpPygCode = 'PMOH' if emp.location.CMiC_Department_ID == 'PMG' else 'CNOH'
        obj.EmpTrdCode = emp.job.name
        obj.EmpWrlCode = 'ATL'
        obj.EmpHourlyRate = 1
        obj.EmpChargeOutRate = obj._determine_charge_rate(emp)
        obj.EmpBillingRate = 3
        obj.EmpSecGrpEmpCode = 'MASTER'
        obj.EmpFilingStatus = '01'
        obj.EmpVUuid = ''
        obj.EmpCountryCode = 'US'
        obj.EmpStateCode = 'GA'
        obj.EmpZipCode = '30152'
        obj.EmpFlsaType = 'N'
        obj.EmpVertexGeocodeSource = 'M'
        obj.EmpType = 'S'
        obj.EmpFullPartTime = 'F'
        obj.EmpSubStatus = 'W'
        obj.EmpUnionized = 'N'
        obj.EmpAdjustedSeriviceDate = '1999-01-02'
        obj.EmpYearWorkingDays = 260.0
        obj.EmpYearWorkingHours = 2080.0
        obj.EmpUeValidFlag = 'Y'
        obj.EmpHomeDeptCode = emp.location.CMiC_Department_ID
        obj.EmpGlAccCode = '600.004'
        obj.EmpPayrollClearAccCode = '240.001'
        obj.EmpDrClearAccCode = '240.001'
        obj.EmpLevAcruGlAccCode = '240.001'
        obj.EmpLevClearAccCode = '240.001'
        obj.EmpAnnualSalary = 100
        obj.EmpPreferPayRate = 'E'
        obj.EmpPreferChargeRate = 'E'
        obj.EmpPreferBillRate = 'E'
        obj.EmpDirectDepMethod = 'N'
        return obj
    def _determine_pyg_Code(self, emp: Employee) -> str:
        if self.EmpTrdCode == 'HCN677': #trade code for Tenant Coordinator
            return 'HRLY'
        if emp.location.CMiC_Department_ID == 'PMG':
            return 'PMOH'
        return 'CNOH'
    def _determine_charge_rate(self, emp: Employee):
        if self.EmpPrnCode == 'W':
            if str(self.EmpNo) == '223543':
                return 89.75
            return config["Union_Charge_Rate"]["rate"]
        return emp.ChargeRate if emp.ChargeRate != 0 else None
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.EmpNo == other.EmpNo
class JCJobCategory:
    def __init__(self, timesheet_entry: CMiC_Timesheet_Entry):
        self.JcatCompCode = timesheet_entry.TshCompCode
        self.JcatJobCode = timesheet_entry.TshJobdeptwoId
        self.JcatPhsCode = timesheet_entry.TshPhsacctwiId
        self.JcatCode = "L"
        self.JcatCatActiveFlag = "Y"
        self.JcatPhaseActiveFlag = "Y"
        self.JcatExclCostWip = "N"
        self.JcatLabourForecastFlag = "N"
        self.JcatCostToComplOvrdFlg = "N"
        self.JcatExclCostBudgWip = "Y"
    def __repr__(self):
        repr_str = f"{self.__class__.__name__}"
        repr_str += '('
        
        for key, val in self.__dict__.items():
            val       = f"'{val}'" if isinstance(val, str) else val
            repr_str += f"{key}={val}\n, "
        
        return repr_str.strip(", ") + ')'
class CMiCTradeCode:
    def __init__(self, ukg_employee: Employee):
        self.TrdCode = ukg_employee.job.name
        self.TrdDescription = ukg_employee.job.description
        self.TrdShortDesc = self.TrdDescription[:10]
        self.TrdEeoClass = "NA"
        self.TrdLastUpdDate = ukg_employee.HireDate
        self.TrdRpAvailblFlag = "Y"
        self.TrdCertFlag = "Y"
        self.TrdType = "T"
        self.TrdUser = "NSMITH"
        self.TrdApprentice = "J"
        self.TrdApprenticeDesc = "Journeyman"