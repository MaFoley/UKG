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
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
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
    WEEKLYPAYGROUP = 'HJRU'
    BIWEEKLYPAYGROUP = 'HJRC'
    def __init__(self, time_entry: Time):
        """
        TshDate expected in yyyy-mm-dd format
        """
        self.TshPrnCode = self._findPayRun(time_entry.paygroup.name)
        self.TshPrnCode = self._findPayRun(time_entry.paygroup.name)
        self.TshDate: str = parser.parse(time_entry.WorkDate).strftime('%Y-%m-%d')
        self.TshPprYear = parser.parse(time_entry.WorkDate).year
        self.TshPprPeriod = self._TshPprPeriod(parser.parse(time_entry.WorkDate))
        self.TshDocumentNo = "-".join([str(time_entry.Id), time_entry.EmpId])
        self.TshEmpNo = time_entry.employee.shortEmpId()
        self.TshUnionCode = None
        if time_entry.job != None:
            self.TshTradeCode = time_entry.job.name #time->emp->jobid->job.name
            self.TshPhsacctwiId = time_entry.orglevel3.name
        else:
            self.TshTradeCode = None
            self.TshPhsacctwiId = None

        if time_entry.project.cmic_project != None and time_entry.project.name[-4:] != "0000":
            self.TshTypeCode = 'J'
            self.TshJobdeptwoId = time_entry.project.cmic_project.JobCode #time->projectid->project.name
        elif time_entry.project.cmic_project != None and time_entry.project.name[-4:] == "0000":
            self.TshTypeCode = 'G'
            self.TshJobdeptwoId = time_entry.project.cmic_project.JobCode #time->projectid->project.name
            #self.TshPhsacctwiId = '600.004'
        else:
            self.TshJobdeptwoId = None
        self.TshNormalHours: float = time_entry.RegHr
        self.TshCompCode = time_entry.companyCode()
        self.TshWorkCompCode = self.TshCompCode
        self.TshCatexpId = 'L'
        self.TshOtHours: float = time_entry.Overt1
        self.TshDotHours:float = time_entry.Overt2
        self.TshWcbCode = None #TODO: attach workers comp code
        self.TshOhType = None

    def _findPayRun(self, paygroup: str) -> str:
        if paygroup.upper().strip() == self.BIWEEKLYPAYGROUP:
            return 'B'
        elif paygroup.upper().strip() == self.WEEKLYPAYGROUP:
            return 'W'
        else:
            return 'New Paygroup'
    def _TshPprPeriod(self, date: datetime) -> int:
        #if tshdate > prior period start date and <= period end date, then return pay period
        periodEndDate = datetime(2024,12,29)
        delta = date - periodEndDate
        return delta.days // 14 + 1
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
class CMiC_Employee:   
    def __init__(self, emp: Employee, effective_date: str):
        #default to new record action code
        self.EmhActionCode = "NR"
        self.EmpCreateAccessCode = "N"
        self.EmhEffectiveDate = effective_date #yyyy-mm-dd
        self.EmpNo= emp.shortEmpId()
        self.EmpPrimaryEmpNo = emp.shortEmpId()
        self.EmpUser = 'NSMITH'
        self.EmpLastName= emp.LastName
        self.EmpFirstName = emp.FirstName
        self.EmpSinNo= '123456789' #plug
        self.EmpStatus = 'A'
        self.EmpDateOfBirth = '1999-01-01' #plug
        self.EmpHireDate = '1999-01-02'#plug
        self.EmpCompCode = 'HJR'
        self.EmpDeptCode = emp.location.CMiC_Department_ID
        self.EmpPrnCode = emp.paygroup.get_cmic_payrun()
        self.EmpPygCode = 'PMOH' if emp.location.CMiC_Department_ID== 'PMG' else 'CNOH'#I think this will work, does it even matter though
        self.EmpTrdCode = emp.job.name 
        self.EmpWrlCode = 'ATL'
        self.EmpHourlyRate = 1
        self.EmpChargeOutRate = emp.ChargeRate if emp.ChargeRate != 0 else None#need provided info
        self.EmpBillingRate = 3 #need provided info
        self.EmpSecGrpEmpCode = 'MASTER'
        self.EmpFilingStatus = '01'
        self.EmpVUuid = ''
        self.EmpCountryCode = 'US'
        self.EmpStateCode = 'GA'
        self.EmpZipCode = '30152'
        self.EmpFlsaType = 'N'
        self.EmpVertexGeocodeSource = 'M'
        self.EmpType = 'S'
        self.EmpFullPartTime = 'F'
        self.EmpSubStatus = 'W'
        self.EmpUnionized = 'N'
        self.EmpAdjustedSeriviceDate = '1999-01-02'
        self.EmpYearWorkingDays = 260.0
        self.EmpYearWorkingHours = 2080.0
        self.EmpUeValidFlag = 'Y'
        self.EmpHomeDeptCode =  emp.location.CMiC_Department_ID # Mapped
        self.EmpGlAccCode = '600.004'
        self.EmpPayrollClearAccCode = '240.001'
        self.EmpDrClearAccCode = '240.001'
        self.EmpLevAcruGlAccCode = '240.001'
        self.EmpLevClearAccCode = '240.001'
        self.EmpAnnualSalary = 100 #I think this is acceptable as a plug unless it has a meaningful impact on charge/bill rate
        self.EmpPreferPayRate = 'E'
        self.EmpPreferChargeRate = 'E'
        self.EmpPreferBillRate = 'E'
        self.EmpDirectDepMethod = 'N'            
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