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
class Project(Base):
    __tablename__ = "Project"
    Id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
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
    JobId: Mapped[int] = mapped_column(ForeignKey("Job.Id"))
    ProjectId: Mapped[int] = mapped_column(ForeignKey("Job.Id"))
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
        return self.EmpId.split('-')[1]
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
            self.TshJobdeptwoId = time_entry.project.name #time->projectid->project.name
            self.TshPhsacctwiId = time_entry.orglevel3.name
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