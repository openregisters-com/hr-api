import re
from typing import Optional, Union
from fastapi import FastAPI, Depends
import sqlite3
import requests
from sqlalchemy import select

from pydantic import BaseModel, Field
from hr_api.database import engine, SessionLocal
import hr_api.models as models
from sqlalchemy.orm import Session
from sqlalchemy import Table, MetaData
from contextlib import contextmanager
from sqlalchemy.exc import IntegrityError
import glob
import os
from dotenv import load_dotenv
import xmltodict
from urllib.parse import quote
from sqlalchemy import text


load_dotenv()

DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER")

app = FastAPI()

models.Base.metadata.create_all(bind=engine)

URLs = [
    (
        "geschlecht",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.geschlecht_2.1/download/GDS.Geschlecht_2.1.json",
    ),
    (
        "rechtsform",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rechtsform_3.4/download/GDS.Rechtsform_3.4.json",
    ),
    (
        "gerichtscode",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xgewerbeanzeige:codeliste:registergerichte_11/download/Registergerichte_11.json",
    ),
    (
        "rollenbezeichnung",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rollenbezeichnung_3.5/download/GDS.Rollenbezeichnung_3.5.json",
    ),
    (
        "eintragungsart",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:reg.eintragungsart_2.0/download/REG.Eintragungsart_2.0.json",
    ),
    (
        "anschriftstyp",
        "https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.anschriftstyp_3.0/download/GDS.Anschriftstyp_3.0.json",
    ),
]


def get_db():
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


@contextmanager
def session_manager():
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


metadata = MetaData()

for entry in URLs:
    url = entry[1]
    name = entry[0]
    response = requests.get(url, timeout=30)
    if response.status_code == 200:
        json_data = response.json()
    else:
        print(f"Failed to retrieve JSON data from the URL: {url}")

    data = json_data["daten"]
    columns = [column["spaltennameTechnisch"] for column in json_data["spalten"]]

    # Convert each list in data to a dictionary using columns for keys
    data_dicts = [dict(zip(columns, item)) for item in data]

    # Get the table object
    table = Table(name, metadata, autoload_with=engine)

    # Insert data into the SQLite database
    for item in data_dicts:
        # SQLAlchemy's insert function can take a dictionary directly
        with session_manager() as session:
            stmt = table.insert().values(**item)
            try:
                session.execute(stmt)
                session.commit()
            except IntegrityError:
                session.rollback()
                continue


class RegisterEntry(BaseModel):

    column: Optional[str]
    position: Optional[str]
    running_number: Optional[str]
    entry_type_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:reg.eintragungsart",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:reg.eintragungsart_2.0/download/REG.Eintragungsart_2.0.json
    )
    text: Optional[str]
    company_number: Optional[str]
    file_path: str


class ParticipantOrganization(BaseModel):

    role_number: Optional[str]
    role_name_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.rollenbezeichnung",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rollenbezeichnung_3.5/download/GDS.Rollenbezeichnung_3.5.json
    )
    name: Optional[str]
    legal_form_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.rechtsform",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rechtsform_3.4/download/GDS.Rechtsform_3.4.json
    )
    city: Optional[str]
    state_code: Optional[str]
    company_number: Optional[str]
    file_path: str


class ParticipantPerson(BaseModel):

    role_number: Optional[str]
    role_name_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.rollenbezeichnung",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rollenbezeichnung_3.5/download/GDS.Rollenbezeichnung_3.5.json
    )
    first_name: Optional[str]
    last_name: Optional[str]
    birth_date: Optional[str]
    gender_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.geschlecht",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.geschlecht_2.1/download/GDS.Geschlecht_2.1.json
    )
    city: Optional[str]
    state_code: Optional[str]
    company_number: Optional[str]
    file_path: str


class Company(BaseModel):
    court_sender_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xunternehmen:codeliste:registergerichte",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xgewerbeanzeige:codeliste:registergerichte_11/download/Registergerichte_11.json
    )
    current_statute_date: Optional[str]
    current_designation: Optional[str]
    legal_form_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.rechtsform",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.rechtsform_3.4/download/GDS.Rechtsform_3.4.json
    )
    location: Optional[str]
    address_type_code: Optional[str] = Field(
        None,
        description="https://www.xrepository.de/details/urn:xoev-de:xjustiz:codeliste:gds.anschriftstyp",  # https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.anschriftstyp_3.0/download/GDS.Anschriftstyp_3.0.json
    )
    street: Optional[str]
    house_number: Optional[str]
    postal_code: Optional[str]
    city: Optional[str]
    state: Optional[str]
    subject_matter: Optional[str]
    register_code: Optional[str]
    register_number: str
    register_number_addition: Optional[str]
    company_number: str
    file_path: str
    opencorporates: str = Field(
        None,
        description="The URL to the company's page on OpenCorporates",
    )


def create_connection():
    connection = sqlite3.connect("structured_information.db")
    return connection


@app.get("/companies/count")
def count_companies(db: Session = Depends(get_db)):
    total = db.query(models.Companies).count()
    return {"total": total}


@app.get("/companies/")
def read_api(
    skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)
):
    return db.query(models.Companies).offset(skip).limit(limit).all()


@app.get("/companies/{company_number}")
def read_company(company_number: str, db: Session = Depends(get_db)):
    result = db.execute(
        select(
            models.Companies.current_statute_date,
            models.Companies.current_designation,
            models.Companies.location,
            models.Companies.street,
            models.Companies.house_number,
            models.Companies.postal_code,
            models.Companies.city,
            models.Companies.state,
            models.Companies.subject_matter,
            models.Companies.register_code,
            models.Companies.register_number,
            models.Companies.register_number_addition,
            models.Companies.company_number,
            models.Companies.file_path,
            models.Companies.opencorporates,
            models.Gerichtscode.XJustiz_Id.label("court_sender_code_value"),
            models.Gerichtscode.Registergericht.label("court_sender_code_label"),
            models.Rechtsform.code.label("legal_form_code_value"),
            models.Rechtsform.wert.label("legal_form_code_label"),
            models.Anschriftstyp.code.label("address_type_code_value"),
            models.Anschriftstyp.wert.label("address_type_code_label"),
        )
        .join(
            models.Gerichtscode,
            models.Companies.court_sender_code == models.Gerichtscode.XJustiz_Id,
        )
        .join(
            models.Rechtsform,
            models.Companies.legal_form_code == models.Rechtsform.code,
        )
        .join(
            models.Anschriftstyp,
            models.Companies.address_type_code == models.Anschriftstyp.code,
        )
        .where(models.Companies.company_number == company_number)
    ).fetchone()

    # Transform the result into the desired format
    return [
        {
            "court_sender_code": {
                "value": result.court_sender_code_value,
                "label": result.court_sender_code_label,
            },
            "current_statute_date": result.current_statute_date,
            "current_designation": result.current_designation,
            "legal_form_code": {
                "value": result.legal_form_code_value,
                "label": result.legal_form_code_label,
            },
            "location": result.location,
            "address_type_code": {
                "value": result.address_type_code_value,
                "label": result.address_type_code_label,
            },
            "street": result.street,
            "house_number": result.house_number,
            "postal_code": result.postal_code,
            "city": result.city,
            "state": result.state,
            "subject_matter": result.subject_matter,
            "register_code": result.register_code,
            "register_number": result.register_number,
            "register_number_addition": result.register_number_addition,
            "company_number": result.company_number,
            "file_path": result.file_path,
            "opencorporates": result.opencorporates,
        }
    ]


@app.get("/register-entries/{company_number}")
def read_entries(company_number: str, db: Session = Depends(get_db)):
    result = db.execute(
        select(
            models.Entries.column,
            models.Entries.position,
            models.Entries.running_number,
            models.Entries.text,
            models.Entries.file_path,
            models.Eintragungsart.Schluessel.label("entry_type_code_value"),
            models.Eintragungsart.Wert.label("entry_type_code_label"),
            models.Companies.company_number.label("company_number_value"),
            models.Companies.current_designation.label("company_number_label"),
        )
        .join(
            models.Eintragungsart,
            models.Entries.entry_type_code == models.Eintragungsart.Schluessel,
        )
        .join(
            models.Companies,
            models.Entries.company_number == models.Companies.company_number,
        )
        .where(models.Entries.company_number == company_number)
    ).fetchall()

    # Transform the result into the desired format
    return [
        {
            "column": row.column,
            "position": row.position,
            "running_number": row.running_number,
            "entry_type_code": {
                "value": row.entry_type_code_value,
                "label": row.entry_type_code_label,
            },
            "text": row.text,
            "company_number": {
                "value": row.company_number_value,
                "label": row.company_number_label,
            },
            "file_path": row.file_path,
        }
        for row in result
    ]


@app.get("/participant-organizations/{company_number}")
def read_participant_organizations(company_number: str, db: Session = Depends(get_db)):
    result = db.execute(
        select(
            models.ParticipantOrganizations.role_number,
            models.ParticipantOrganizations.name,
            models.ParticipantOrganizations.city,
            models.ParticipantOrganizations.state_code,
            models.ParticipantOrganizations.file_path,
            models.Rollenbezeichnung.code.label("role_name_code_value"),
            models.Rollenbezeichnung.wert.label("role_name_code_label"),
            models.Rechtsform.code.label("legal_form_code_value"),
            models.Rechtsform.wert.label("legal_form_code_label"),
            models.Companies.company_number.label("company_number_value"),
            models.Companies.current_designation.label("company_number_label"),
        )
        .join(
            models.Rollenbezeichnung,
            models.ParticipantOrganizations.role_name_code
            == models.Rollenbezeichnung.code,
        )
        .join(
            models.Rechtsform,
            models.ParticipantOrganizations.legal_form_code == models.Rechtsform.code,
        )
        .join(
            models.Companies,
            models.ParticipantOrganizations.company_number
            == models.Companies.company_number,
        )
        .where(models.ParticipantOrganizations.company_number == company_number)
    ).fetchall()

    # Transform the result into the desired format
    return [
        {
            "role_number": row.role_number,
            "role_name_code": {
                "value": row.role_name_code_value,
                "label": row.role_name_code_label,
            },
            "name": row.name,
            "legal_form_code": {
                "value": row.legal_form_code_value,
                "label": row.legal_form_code_label,
            },
            "city": row.city,
            "state_code": row.state_code,
            "company_number": {
                "value": row.company_number_value,
                "label": row.company_number_label,
            },
            "file_path": row.file_path,
        }
        for row in result
    ]


@app.get("/participant-persons/{company_number}")
def read_participant_persons(company_number: str, db: Session = Depends(get_db)):
    result = db.execute(
        select(
            models.ParticipantPersons.role_number,
            models.ParticipantPersons.first_name,
            models.ParticipantPersons.last_name,
            models.ParticipantPersons.birth_date,
            models.ParticipantPersons.city,
            models.ParticipantPersons.state_code,
            models.ParticipantPersons.file_path,
            models.Rollenbezeichnung.code.label("role_name_code_value"),
            models.Rollenbezeichnung.wert.label("role_name_code_label"),
            models.Geschlecht.code.label("gender_code_value"),
            models.Geschlecht.wert.label("gender_code_label"),
            models.Companies.company_number.label("company_number_value"),
            models.Companies.current_designation.label("company_number_label"),
        )
        .join(
            models.Rollenbezeichnung,
            models.ParticipantPersons.role_name_code == models.Rollenbezeichnung.code,
        )
        .join(
            models.Geschlecht,
            models.ParticipantPersons.gender_code == models.Geschlecht.code,
        )
        .join(
            models.Companies,
            models.ParticipantPersons.company_number == models.Companies.company_number,
        )
        .where(models.ParticipantPersons.company_number == company_number)
    ).fetchall()

    # Transform the result into the desired format
    return [
        {
            "role_number": row.role_number,
            "role_name_code": {
                "value": row.role_name_code_value,
                "label": row.role_name_code_label,
            },
            "first_name": row.first_name,
            "last_name": row.last_name,
            "birth_date": row.birth_date,
            "gender_code": {
                "value": row.gender_code_value,
                "label": row.gender_code_label,
            },
            "city": row.city,
            "state_code": row.state_code,
            "company_number": {
                "value": row.company_number_value,
                "label": row.company_number_label,
            },
            "file_path": row.file_path,
        }
        for row in result
    ]


@app.post("/companies/")
def create_company(company: Company, db: Session = Depends(get_db)):
    db_company = models.Companies(
        court_sender_code=company.court_sender_code,
        current_statute_date=company.current_statute_date,
        current_designation=company.current_designation,
        legal_form_code=company.legal_form_code,
        location=company.location,
        address_type_code=company.address_type_code,
        street=company.street,
        house_number=company.house_number,
        postal_code=company.postal_code,
        city=company.city,
        state=company.state,
        subject_matter=company.subject_matter,
        register_code=company.register_code,
        register_number=company.register_number,
        register_number_addition=company.register_number_addition,
        company_number=company.company_number,
        file_path=company.file_path,
        opencorporates=company.opencorporates,
    )
    db.add(db_company)
    db.commit()
    db.refresh(db_company)
    return db_company


def extract_company_info(data_dict, latest_file_path):
    # Extract the required information
    company = data_dict.get("tns:nachricht.reg.0400003", {})
    nachrichtenkopf = company.get("tns:nachrichtenkopf", {})
    auswahl_absender = nachrichtenkopf.get("tns:auswahl_absender", {})
    absender_gericht = auswahl_absender.get("tns:absender.gericht", {})
    court_sender_code = absender_gericht.get("code")

    fachdatenRegister = company.get("tns:fachdatenRegister", {})
    basisdatenRegister = fachdatenRegister.get("tns:basisdatenRegister", {})
    satzungsdatum = basisdatenRegister.get("tns:satzungsdatum", {})
    current_statute_date = satzungsdatum.get("tns:aktuellesSatzungsdatum")

    rechtstraeger = basisdatenRegister.get("tns:rechtstraeger", {})
    bezeichnung = rechtstraeger.get("tns:bezeichnung", {})
    current_designation = bezeichnung.get("tns:bezeichnung.aktuell")

    angabenZurRechtsform = rechtstraeger.get("tns:angabenZurRechtsform", {})
    rechtsform = angabenZurRechtsform.get("tns:rechtsform", {})
    legal_form_code = rechtsform.get("code")

    sitz = rechtstraeger.get("tns:sitz", {})
    location = sitz.get("tns:ort")

    anschrift = rechtstraeger.get("tns:anschrift", {})
    if isinstance(anschrift, list):
        anschrift = anschrift[0] if anschrift else {}
    anschriftstyp = anschrift.get("tns:anschriftstyp", {})
    address_type_code = anschriftstyp.get("code")

    street = anschrift.get("tns:strasse")
    house_number = anschrift.get("tns:hausnummer")
    postal_code = anschrift.get("tns:postleitzahl")
    city = anschrift.get("tns:ort")

    staat = anschrift.get("tns:staat", {})
    auswahl_staat = staat.get("tns:auswahl_staat", {})
    staat = auswahl_staat.get("tns:staat", {})
    state = staat.get("code")

    subject_matter = basisdatenRegister.get("tns:gegenstand")

    grunddaten = company.get("tns:grunddaten", {})
    verfahrensdaten = grunddaten.get("tns:verfahrensdaten", {})
    instanzdaten = verfahrensdaten.get("tns:instanzdaten", {})
    aktenzeichen = instanzdaten.get("tns:aktenzeichen", {})
    auswahl_aktenzeichen = aktenzeichen.get("tns:auswahl_aktenzeichen", {})
    aktenzeichen_strukturiert = auswahl_aktenzeichen.get(
        "tns:aktenzeichen.strukturiert", {}
    )
    register = aktenzeichen_strukturiert.get("tns:register", {})
    register_code = register.get("code")
    register_number = aktenzeichen_strukturiert.get("tns:laufendeNummer")
    register_number_addition = aktenzeichen_strukturiert.get("tns:zusatz")

    if not register_number:
        # TODO take apart the freitext
        register_number = aktenzeichen_strukturiert.get("tns:aktenzeichen.freitext")
        if not register_number:
            register_number = (
                data_dict.get("tns:nachricht.reg.0400003", {})
                .get("tns:nachrichtenkopf", {})
                .get("tns:aktenzeichen.absender")
            )
        # Regex pattern
        pattern = r"(HRB)\s+(\d+)\s+([A-Z]+)"

        # Search for the pattern in the register_number
        match = re.search(pattern, register_number)
        if match:
            register_code = match.group(1)
            register_number = match.group(2)
            register_number_addition = match.group(3)

    if register_number_addition:
        company_number = f"{court_sender_code}_{register_code}{register_number}{register_number_addition}"
    else:
        company_number = f"{court_sender_code}_{register_code}{register_number}"

    # Merge all the information into a single dictionary
    company = Company(
        court_sender_code=court_sender_code,
        current_statute_date=current_statute_date,
        current_designation=current_designation,  # TODO sometimes the name is inside Parties..
        legal_form_code=legal_form_code,
        location=location,
        address_type_code=address_type_code,
        street=street,
        house_number=house_number,
        postal_code=postal_code,
        city=city,
        state=state,
        subject_matter=subject_matter,
        register_code=register_code,
        register_number=register_number,
        register_number_addition=register_number_addition,
        company_number=company_number,
        file_path=latest_file_path,
        opencorporates=f"https://www.opencorporates.com/companies/de/{company_number}",
    )

    return company


def extract_parties(data_dict, company_number, latest_file_path):
    # Extract the people and organizations under tns:beteiligung
    company = data_dict.get("tns:nachricht.reg.0400003", {})
    grunddaten = company.get("tns:grunddaten", {})
    verfahrensdaten = grunddaten.get("tns:verfahrensdaten", {})
    beteiligung = verfahrensdaten.get("tns:beteiligung", [])

    parties = []
    for participant in beteiligung:
        roles = participant.get("tns:rolle", {})
        # If roles is a list, take the first one
        if isinstance(roles, list):
            role = roles[0]
        else:
            role = roles
        role_number = role.get("tns:rollennummer")
        rollenbezeichnung = role.get("tns:rollenbezeichnung", {})
        role_name_code = (
            rollenbezeichnung.get("code") if rollenbezeichnung is not None else None
        )
        beteiligter = participant.get("tns:beteiligter", {})
        auswahl_beteiligter = beteiligter.get("tns:auswahl_beteiligter", {})
        if "tns:natuerlichePerson" in auswahl_beteiligter:
            person_info = auswahl_beteiligter.get("tns:natuerlichePerson", {})
            vollerName = person_info.get("tns:vollerName", {})
            first_name = vollerName.get("tns:vorname")
            last_name = vollerName.get("tns:nachname")
            geburt = person_info.get("tns:geburt", {})
            birth_date = geburt.get("tns:geburtsdatum") if geburt is not None else None
            geschlecht = person_info.get("tns:geschlecht", {})
            gender_code = geschlecht.get("code") if geschlecht is not None else None
            anschrift = person_info.get("tns:anschrift", {})
            city = anschrift.get("tns:ort")
            staat = anschrift.get("tns:staat", {})
            auswahl_staat = staat.get("tns:auswahl_staat", {})
            staat = auswahl_staat.get("tns:staat", {})
            state_code = staat.get("code") if staat is not None else None

            parties.append(
                ParticipantPerson(
                    role_number=role_number,
                    role_name_code=role_name_code,
                    first_name=first_name,
                    last_name=last_name,
                    birth_date=birth_date,
                    gender_code=gender_code,
                    city=city,
                    state_code=state_code,
                    company_number=company_number,
                    file_path=latest_file_path,
                )
            )
        elif "tns:organisation" in auswahl_beteiligter:
            org_info = auswahl_beteiligter.get("tns:organisation", {})
            bezeichnung = org_info.get("tns:bezeichnung", {})
            name = bezeichnung.get("tns:bezeichnung.aktuell")
            angabenZurRechtsform = org_info.get("tns:angabenZurRechtsform", {})
            rechtsform = angabenZurRechtsform.get("tns:rechtsform", {})
            legal_form_code = rechtsform.get("code")
            sitz = org_info.get("tns:sitz", {})
            city = sitz.get("tns:ort")
            anschrift = org_info.get("tns:anschrift", {})
            staat = anschrift.get("tns:staat", {})
            auswahl_staat = staat.get("tns:auswahl_staat", {})
            staat = auswahl_staat.get("tns:staat", {})
            state_code = staat.get("code")
            parties.append(
                ParticipantOrganization(
                    role_number=role_number,
                    role_name_code=role_name_code,
                    name=name,
                    legal_form_code=legal_form_code,
                    city=city,
                    state_code=state_code,
                    company_number=company_number,
                    file_path=latest_file_path,
                )
            )

    return parties


def extract_entries(data_dict, company_number, latest_file_path):
    # Extract the tns:eintragungstext
    company = data_dict.get("tns:nachricht.reg.0400003", {})
    fachdatenRegister = company.get("tns:fachdatenRegister", {})
    auszug = fachdatenRegister.get("tns:auszug", {})
    eintragungstext = auszug.get("tns:eintragungstext", [])

    entries = []
    for entry in eintragungstext:
        if isinstance(entry, dict):
            column = entry.get("tns:spalte")
            position = entry.get("tns:position")
            running_number = entry.get("tns:laufendeNummer")
            eintragungsart = entry.get("tns:eintragungsart", {})
            entry_type_code = eintragungsart.get("code")
            text = entry.get("tns:text")
            entries.append(
                RegisterEntry(
                    column=column,
                    position=position,
                    running_number=running_number,
                    entry_type_code=entry_type_code,
                    text=text,
                    company_number=company_number,
                    file_path=latest_file_path,
                )
            )
    return entries


@app.get("/admin/refresh-db")
def refresh_db(db: Session = Depends(get_db)):
    db.execute(text("DELETE FROM companies"))
    db.execute(text("DELETE FROM entries"))
    db.execute(text("DELETE FROM participant_organizations"))
    db.execute(text("DELETE FROM participant_persons"))

    company_dirs = glob.glob(f"/{DOWNLOAD_FOLDER}/*/*/")
    for company_dir in company_dirs:
        # Get a list of all matching file paths for the current company
        xml_files = glob.glob(f"{company_dir}si/*.xml")
        xhtml_files = glob.glob(f"{company_dir}si/*.xhtml")
        file_paths = xml_files + xhtml_files

        if not file_paths:
            continue

        # Sort the file paths by date and time in descending order
        file_paths.sort(
            key=lambda f: re.search(r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}", f).group(0),
            reverse=True,
        )

        # Pick the latest file
        latest_file_path = file_paths[0]

        with open(latest_file_path, "r", encoding="utf-8") as file:
            xml_data = file.read()

        # Parse the XML data
        data_dict = xmltodict.parse(xml_data)

        url_path = (
            "https://commandcenter.hippocampus-vector.ts.net:10000/download/"
            + quote(latest_file_path.replace("/root/download/", ""))
        )

        if "tns:nachricht.reg.0400003" in data_dict:
            company = extract_company_info(data_dict, url_path)
            parties = extract_parties(data_dict, company.company_number, url_path)
            entries = extract_entries(data_dict, company.company_number, url_path)
        else:
            print(f"File {latest_file_path} does not contain the required data")

        if company.current_designation is None:
            for party in parties:
                if isinstance(party, ParticipantOrganization):
                    if party.role_name_code == "287":  # ["287","Rechtsträger(in)",""]
                        company.current_designation = party.name
                        break

        # Insert company into the database
        db.add(models.Companies(**company.model_dump()))

        for party in parties:
            party_values = party.model_dump()
            if isinstance(party, models.ParticipantPersons):
                db.add(models.ParticipantPersons(**party_values))
            elif isinstance(party, models.ParticipantOrganizations):
                db.add(models.ParticipantOrganizations(**party_values))

        for entry_item in entries:
            entry_values = entry_item.model_dump()
            db.add(models.Entries(**entry_values))

    db.commit()
    db.close()

    return {"message": f"Added {len(company_dirs)} companies to the database.."}


@app.get("/analytics/company-with-ownershiptable/count")
def count_company_with_ownership_table():
    company_dirs = glob.glob(f"/{DOWNLOAD_FOLDER}/*/*/")
    count = 0
    for company_dir in company_dirs:
        if (
            glob.glob(f"{company_dir}dk/list_of_shareholders/*.pdf")
            or glob.glob(f"{company_dir}dk/list_of_shareholders/*.tif")
            or glob.glob(f"{company_dir}dk/list_of_shareholders/*.tiff")
        ):
            count += 1
    return {"companies": count}


@app.get("/analytics/ownership-tables/count")
def count_ownership_tables():
    company_dirs = glob.glob(f"/{DOWNLOAD_FOLDER}/*/*/")
    count = 0
    for company_dir in company_dirs:
        files = glob.glob(f"{company_dir}dk/list_of_shareholders/*")
        for file in files:
            if not file.endswith(".json"):
                count += 1
    return {"companies": count}


@app.get("/analytics/register-numbers/count")
def count_registernumbers():
    return {"register-numbers": len(glob.glob(f"/{DOWNLOAD_FOLDER}/*/"))}
