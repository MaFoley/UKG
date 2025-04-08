from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
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
    WorkDate: Mapped[str]
    PaycodeId: Mapped[int] 
    PaycodeExp: Mapped[str]
    InOrg: Mapped[str]
    OutOrg: Mapped[str]
    In: Mapped[float] 
    Out: Mapped[float] 
    InExp: Mapped[float] 
    OutExp: Mapped[float] 
    SchHrs: Mapped[float] 
    Overt1: Mapped[float] 
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
#    PaygroupId: Mapped[int] = mapped_column(ForeignKey("Paygroup.Id"))
    LocationId: Mapped[int] = mapped_column(ForeignKey("Location.Id"))
    JobId: Mapped[int] = mapped_column(ForeignKey("Job.Id"))
    ProjectId: Mapped[int] = mapped_column(ForeignKey("Job.Id"))
    OrgLevel1Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel1.Id"))
    OrgLevel2Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel2.Id"))
    OrgLevel3Id: Mapped[int] = mapped_column(ForeignKey("OrgLevel3.Id"))
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
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, EmpId={self.EmpId!r}"
class Employee(Base):
    __tablename__ = "Employee"
    EmpId: Mapped[str] = mapped_column(primary_key=True)
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
    OrgLevel4Id: Mapped[int]
    PayPolicyId: Mapped[int]
    ShiftId: Mapped[int]
    AccessGroupId: Mapped[int]
    def __repr__(self) -> str:
        return f"(Id={self.Id!r}, EmpId={self.EmpId!r}, Name: {self.FirstName} {self.LastName})"