# HR-API

## Overview
HR-API is a lightweight and efficient API designed to parse XML files from the German Handelsregister (Commercial Register). It extracts structured information from Handelsregister XML data, making it easier to process and integrate into your applications.

## Features
- Parses XML files from the Handelsregister
- Extracts key structured information
- Converts Handelsregister data into an easy-to-use format
- Provides a RESTful API for querying parsed data
- Designed for performance and scalability

## Installation
To install and run HR-API, follow these steps:

```sh
# Clone the repository
git clone https://github.com/yourusername/hr-api.git
cd hr-api

# Install dependencies using Poetry
poetry install

# Activate the virtual environment
poetry shell

# Start the API
python main.py
```

## Usage
### Parsing Handelsregister XML
Upload an XML file to extract structured data:

```sh
curl -X POST -F "file=@handelsregister.xml" http://localhost:5000/parse
```

### Sample Response
```json
{
  "company_name": "Muster GmbH",
  "registration_number": "HRB 123456",
  "address": "Musterstraße 1, 10115 Berlin",
  "managing_directors": ["Max Mustermann", "Erika Musterfrau"],
  "status": "Active"
}
```

## Configuration
You can configure the API settings in `config.yaml`:
```yaml
server:
  host: "0.0.0.0"
  port: 5000

logging:
  level: "INFO"
```

## Roadmap
- ✅ Initial XML parsing support
- ✅ REST API integration
- ⏳ Database storage for parsed data
- ⏳ Web-based UI for data visualization
- ⏳ Advanced search and filtering

## Contributions
Contributions are welcome! Feel free to fork this repository and submit pull requests.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact
For questions or support, open an issue or reach out via email: your.email@example.com
