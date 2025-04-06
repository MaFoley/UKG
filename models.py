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
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"
class OrgLevel1(Base):
    __tablename__ = "OrgLevel1"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"
class OrgLevel2(Base):
    __tablename__ = "OrgLevel2"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"
class OrgLevel3(Base):
    __tablename__ = "OrgLevel3"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"
class Project(Base):
    __tablename__ = "Project"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"
class Location(Base):
    __tablename__ = "Location"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String())
    description: Mapped[str] = mapped_column(String())
    def __repr__(self) -> str:
        return f"(id={self.id!r}, name={self.name!r}, description={self.description!r})"