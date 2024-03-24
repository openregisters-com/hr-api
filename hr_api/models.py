from sqlalchemy import Column, Integer, String, ForeignKey
from hr_api.database import Base


class Companies(Base):
    __tablename__ = "companies"
    court_sender_code = Column(String, ForeignKey("gerichtscode.XJustiz_Id"))
    current_statute_date = Column(String)
    current_designation = Column(String)
    legal_form_code = Column(String, ForeignKey("rechtsform.code"))
    location = Column(String)
    address_type_code = Column(String, ForeignKey("anschriftstyp.code"))
    street = Column(String)
    house_number = Column(String)
    postal_code = Column(String)
    city = Column(String)
    state = Column(String)
    subject_matter = Column(String)
    register_code = Column(String)
    register_number = Column(String)
    register_number_addition = Column(String)
    company_number = Column(String, primary_key=True, index=True)
    file_path = Column(String)
    opencorporates = Column(String)


class ParticipantPersons(Base):
    __tablename__ = "participant_persons"
    role_number = Column(String)
    role_name_code = Column(String, ForeignKey("rollenbezeichnung.code"))
    first_name = Column(String)
    last_name = Column(String)
    birth_date = Column(String)
    gender_code = Column(String, ForeignKey("geschlecht.code"))
    city = Column(String)
    state_code = Column(String)
    company_number = Column(
        String, ForeignKey("companies.company_number"), primary_key=True
    )
    file_path = Column(String)


class ParticipantOrganizations(Base):
    __tablename__ = "participant_organizations"
    role_number = Column(String)
    role_name_code = Column(String, ForeignKey("rollenbezeichnung.code"))
    name = Column(String)
    legal_form_code = Column(String, ForeignKey("rechtsform.code"))
    city = Column(String)
    state_code = Column(String)
    company_number = Column(
        String, ForeignKey("companies.company_number"), primary_key=True
    )
    file_path = Column(String)


class Entries(Base):
    __tablename__ = "entries"
    column = Column(String)
    position = Column(String)
    running_number = Column(String)
    entry_type_code = Column(String, ForeignKey("eintragungsart.Schluessel"))
    text = Column(String)
    company_number = Column(
        String, ForeignKey("companies.company_number"), primary_key=True
    )
    file_path = Column(String)


class Geschlecht(Base):
    __tablename__ = "geschlecht"
    code = Column(String, primary_key=True)
    wert = Column(String)
    beschreibung = Column(String)


class Rechtsform(Base):
    __tablename__ = "rechtsform"
    code = Column(String, primary_key=True)
    wert = Column(String)
    beschreibung = Column(String)


class Gerichtscode(Base):
    __tablename__ = "gerichtscode"
    XJustiz_Id = Column(String, primary_key=True)
    Registergericht = Column(String)
    Art = Column(String)
    Land = Column(String)
    PLZ = Column(String)
    gueltigBis = Column(String)
    kuenftigZuVerwendendeCodes = Column(String)


class Rollenbezeichnung(Base):
    __tablename__ = "rollenbezeichnung"
    code = Column(String, primary_key=True)
    wert = Column(String)
    fachmodul = Column(String)


class Eintragungsart(Base):
    __tablename__ = "eintragungsart"
    Schluessel = Column(String, primary_key=True)
    Wert = Column(String)


class Anschriftstyp(Base):
    __tablename__ = "anschriftstyp"
    code = Column(String, primary_key=True)
    wert = Column(String)
