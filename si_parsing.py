# %%
import xmltodict
from pydantic import BaseModel, Field
from typing import Optional
import glob
import re
import sqlite3
import os
import logging
from urllib.parse import quote
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("/root/hr-crawl-service-playwright/logs.log"),
        logging.StreamHandler()
    ],
)

# Create a logger instance
logger = logging.getLogger(__name__)

# Log an example message


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
    # TODO there is more address data as laid out here, like here: https://commandcenter.hippocampus-vector.ts.net:10000/download/190285/Aerocene%20Foundation%20gGmbH/si/2024-03-11T13-59-19.xml


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


# %%
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


# %%


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


if __name__ == "__main__":

    conn = sqlite3.connect("structured_information.db")
    cursor = conn.cursor()

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

    conn = sqlite3.connect("structured_information.db")
    cursor = conn.cursor()

    tables = {
        "geschlecht": """
                CREATE TABLE geschlecht(
                    code TEXT PRIMARY KEY,
                    wert TEXT,
                    beschreibung TEXT
                )
            """,
        "rechtsform": """
                CREATE TABLE rechtsform(
                    code TEXT PRIMARY KEY,
                    wert TEXT,
                    beschreibung TEXT
                )
            """,
        "gerichtscode": """
                CREATE TABLE gerichtscode(
                    XJustiz_Id TEXT PRIMARY KEY,
                    Registergericht TEXT,
                    Art TEXT,
                    Land TEXT,
                    PLZ TEXT,
                    gueltigBis TEXT,
                    kuenftigZuVerwendendeCodes TEXT
                )
            """,
        "rollenbezeichnung": """
                CREATE TABLE rollenbezeichnung(                
                    code TEXT PRIMARY KEY,
                    wert TEXT,
                    fachmodul TEXT
                )
            """,
        "eintragungsart": """
                CREATE TABLE eintragungsart(                
                    Schluessel TEXT PRIMARY KEY,
                    Wert TEXT
                )
            """,
        "anschriftstyp": """
                CREATE TABLE anschriftstyp(
                    code TEXT PRIMARY KEY,
                    wert TEXT
                )
                """,
    }

    for table_name, create_table_statement in tables.items():
        # Check if the table exists
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        table_exists = cursor.fetchone()

        # If the table exists, drop it
        if table_exists:
            cursor.execute(f"DROP TABLE {table_name}")

        # Create the table
        cursor.execute(create_table_statement)

    for entry in URLs:
        url = entry[1]
        name = entry[0]
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
        else:
            print(f"Failed to retrieve JSON data from the URL: {url}")

        data = json_data["daten"]
        columns = [column["spaltennameTechnisch"] for column in json_data["spalten"]]

        # Convert each list in data to a dictionary using columns for keys
        data_dicts = [dict(zip(columns, item)) for item in data]

        # Insert data into the SQLite database
        for item in data_dicts:
            columns = ", ".join(item.keys())
            placeholders = ", ".join(["?"] * len(item))
            values = tuple(item.values())
            cursor.execute(
                f"INSERT INTO {name} ({columns}) VALUES ({placeholders})", values
            )
    # Commit the changes to the database
    conn.commit()

    # List of tables to create
    tables = {
        "companies": """
            CREATE TABLE companies(
                court_sender_code TEXT,
                current_statute_date TEXT,
                current_designation TEXT,
                legal_form_code TEXT,
                location TEXT,
                address_type_code TEXT,
                street TEXT,
                house_number TEXT,
                postal_code TEXT,
                city TEXT,
                state TEXT,
                subject_matter TEXT,
                register_code TEXT,
                register_number TEXT,
                register_number_addition TEXT,
                company_number TEXT PRIMARY KEY,
                file_path TEXT,
                opencorporates TEXT,
                FOREIGN KEY (court_sender_code) REFERENCES gerichtscode(XJustiz_Id),
                FOREIGN KEY (legal_form_code) REFERENCES rechtsform(code),
                FOREIGN KEY (address_type_code) REFERENCES anschriftstyp(code)
            )
        """,
        "participant_persons": """
            CREATE TABLE participant_persons(
                role_number TEXT,
                role_name_code TEXT,
                first_name TEXT,
                last_name TEXT,
                birth_date TEXT,
                gender_code TEXT,
                city TEXT,
                state_code TEXT,
                company_number TEXT,
                file_path TEXT,
                FOREIGN KEY (role_name_code) REFERENCES rollenbezeichnung(code),
                FOREIGN KEY (company_number) REFERENCES companies(company_number),
                FOREIGN KEY (gender_code) REFERENCES geschlecht(code)
            )
        """,
        "participant_organizations": """
            CREATE TABLE participant_organizations(
                role_number TEXT,
                role_name_code TEXT,
                name TEXT,
                legal_form_code TEXT,
                city TEXT,
                state_code TEXT,
                company_number TEXT,
                file_path TEXT,
                FOREIGN KEY (role_name_code) REFERENCES rollenbezeichnung(code),
                FOREIGN KEY (legal_form_code) REFERENCES rechtsform(code),
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        """,
        "entries": """
            CREATE TABLE entries(
                column TEXT,
                position TEXT,
                running_number TEXT,
                entry_type_code TEXT,
                text TEXT,
                company_number TEXT,
                file_path TEXT,
                FOREIGN KEY (company_number) REFERENCES companies(company_number),
                FOREIGN KEY (entry_type_code) REFERENCES eintragungsart(Schluessel)
            )
        """,
    }

    # For each table
    for table_name, create_table_statement in tables.items():
        # Check if the table exists
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        table_exists = cursor.fetchone()

        # If the table exists, drop it
        if table_exists:
            cursor.execute(f"DROP TABLE {table_name}")

        # Create the table
        cursor.execute(create_table_statement)

    # Get a list of all company directories
    company_dirs = glob.glob("/root/download/*/*/")

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

        with open(latest_file_path, "r") as file:
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
                        # TODO add more information from the organization to the company, mainly address https://commandcenter.hippocampus-vector.ts.net:10000/download/190285/Aerocene%20Foundation%20gGmbH/si/2024-03-11T13-59-19.xml
                        break

        cursor.execute(
            """
            INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(company.model_dump().values()),
        )

        for party in parties:
            party_values = list(party.model_dump().values())
            if isinstance(party, ParticipantPerson):
                cursor.execute(
                    """
                    INSERT INTO participant_persons VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(party_values),
                )
            elif isinstance(party, ParticipantOrganization):
                cursor.execute(
                    """
                    INSERT INTO participant_organizations VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(party_values),
                )

        for entry in entries:
            entry_values = list(entry.model_dump().values())
            cursor.execute(
                """
                INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(entry_values),
            )

        logger.info(f"{latest_file_path} has been processed.")
    conn.commit()
    conn.close()
