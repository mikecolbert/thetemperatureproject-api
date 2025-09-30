# thetemperatureproject-api

API server for thetemperatureproject

## Installation

1. Clone this repository to local computer

2. Create a new virtual environment

   - Windows: `python -m venv ./venv`
   - Mac: `python3 -m venv ./venv`

3. Activate the new virtual environment

   - Windows: `.\venv\Scripts\activate`
   - Mac: `source ./venv/bin/activate`

4. Install the dependencies `pip install -r requirements.txt`

5. Run the application with `flask run` or `python app.py`

## Local testing and development

1. For local testing and development you will need to create a .env file that contains details about connecting to your database server.

_Example .env file_

```
DB_HOST = URL of your MySQL server
DB_NAME = name of the database on the server
DB_USER = username to log into database
DB_PASS = password
SECRET_KEY = long random string
```

2. For local testing you may also want to set `debug=True` in the last line of code.

---

## Configure certificates:

https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-root-certificate-rotation#how-to-update-the-root-certificate-store-on-your-client

Download the three necessary cerficates linked at this url.  
Scroll down and it shows the order to paste each certificate into the combined-ca-certificates.pem file. You need to convert the Microsoft RSA certificate to a .pem file using these instructions.

On a Mac, rename the Microsoft RSA certificate to remove the spaces.  
Run the following to convert it to a .pem  
`openssl x509 -inform DER -in MicrosoftRSARootCertificateAuthority2017.crt -out MicrosoftRSARootCertificateAuthority2017.crt.pem`
Paste it into combined-ca-certificates.pem
