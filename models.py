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
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class Location(Base):
    __tablename__ = "Location"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, name={self.name!r}, description={self.description!r})"
class Time(Base):
    __tablename__ = "Time"
    Id: Mapped[int] = mapped_column(primary_key=True)
    EmpId: Mapped[int] = mapped_column(ForeignKey("Employee.EmpId"))
    employee: Mapped["Employee"] = relationship(back_populates="time_entries")
    WorkDate: Mapped[str]
    PaycodeId: Mapped[int] 
    PaycodeExp: Mapped[str]
    InOrg: Mapped[str]
    OutOrg: Mapped[str]
    In: Mapped[float] 
    Out: Mapped[float] 
    InExp: Mapped[float] 
    OutExp: Mapped[float] 
    RegHr: Mapped[float] 
    SchHrs: Mapped[float] 
    Overt1: Mapped[float] 
    Overt2: Mapped[float] 
    Overt3: Mapped[float] 
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
    ReasonId: Mapped[str] 
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
    SiteIn: Mapped[float]
    SiteOut: Mapped[float]
    QuanGood: Mapped[float]
    QuanScrap: Mapped[float]
    BegSched: Mapped[float]
    EndSched: Mapped[float]
    Status: Mapped[float]
    AdjustmentDate: Mapped[float]
    InRounded: Mapped[str]
    OutRounded: Mapped[str]
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
    BirthDate: Mapped[str]
    HireDate: Mapped[str]
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
class Timesheet_Entry:
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
        self.TshDocumentNo = None
        self.TshEmpNo = time_entry.employee.shortEmpId()
        self.TshTypeCode = 'J'
        self.TshUnionCode = None
        if time_entry.job != None:
            self.TshTradeCode = time_entry.job.name #time->emp->jobid->job.name
            self.TshPhsacctwiId = time_entry.orglevel3.name
        if time_entry.project.cmic_project != None:
            self.TshJobdeptwoId = time_entry.project.cmic_project.JobCode #time->projectid->project.name
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
    def __init__(self, emp: Employee):
        dept_map_df = pd.read_csv("DataFiles/DEPARTMENT MAP.csv", header=0, usecols=[1, 3])
        dept_map_df.columns = ['UKGDName', 'CMiCD']
        dept_map = {
            str(k).strip(): str(v).strip()
            for k, v in zip(dept_map_df['UKGDName'], dept_map_df['CMiCD'])
        }       
        #self.EmhActionCode = "NR"
        _mapped_dept = dept_map.get(emp.location.name, emp.location.name)  # Mapped
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
        self.EmpDeptCode = _mapped_dept
        self.EmpPrnCode = 'B'
        self.EmpPygCode = 'PMOH' if _mapped_dept== 'PMG' else 'CNOH'#I think this will work, does it even matter though
        self.EmpTrdCode = emp.job.name, 
        self.EmpWrlCode = 'ATL'
        self.EmpChargeOutRate = 3 #need provided info
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
        self.EmpHomeDeptCode = _mapped_dept  # Mapped
        self.EmpGlAccCode = '600.004'
        self.EmpPayrollClearAccCode = '240.001'
        self.EmpDrClearAccCode = '240.001'
        self.EmpLevAcruGlAccCode = '240.001'
        self.EmpLevClearAccCode = '240.001'
        self.EmpAnnualSalary = 100 #I think this is acceptable as a plug unless it has a meaningful impact on charge/bill rate
        self.EmpPreferPayRate = 'J'
        self.EmpPreferChargeRate = 'J'
        self.EmpPreferBillRate = 'J'
        self.EmpDirectDepMethod = 'N'            
