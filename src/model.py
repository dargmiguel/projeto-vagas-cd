from sqlalchemy import Column, Integer, String, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Vaga(Base):
    __tablename__ = 'vagas'

    id = Column(Integer, primary_key=True, index=True)
    companyId = Column(Integer, nullable=True)
    name = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    careerPageName = Column(String, nullable=True)
    careerPageLogo = Column(String, nullable=True)
    careerPageUrl = Column(String, nullable=True)
    publishedDate = Column(String, nullable=True)
    applicationDeadline = Column(String, nullable=True)
    isRemoteWork = Column(Boolean, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    jobUrl = Column(String, unique=True, nullable=True)
    workplaceType = Column(String, nullable=True)
    disabilities = Column(Boolean, nullable=True)
    skills = Column(Text, nullable=True)  # armazenar JSON como texto

    def __repr__(self):
        return f"<Vaga(id={self.id}, name={self.name}, companyId={self.companyId})>"
