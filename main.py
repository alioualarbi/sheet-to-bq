"""Import Sheets data into BQ."""

import collections
import csv
import datetime
import logging
import os.path
import pickle
import tempfile

import flask

from google.api_core import exceptions
from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build

# API details
_SHEETS_SERVICE_NAME = 'sheets'
_SHEETS_SERVICE_API_VERSION = 'v4'

_DRIVE_SERVICE_NAME = 'drive'
_DRIVE_SERVICE_API_VERSION = 'v3'

# If modifying these scopes, delete the file token.pickle.
_SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigquery.insertdata',
]

_CREDENTIALS_FILENAME = 'credentials.json'
_CREDENTIALS_GCS_BUCKET_NAME = 'sheets_to_bq'
_CREDENTIALS_GCS_FILENAME = 'credentials.json'
_TOKEN_FILENAME = 'token.pickle'

# The ID and range of a sample spreadsheet.
_MIME_TYPE = 'application/vnd.google-apps.spreadsheet'
_SPREADSHEET_ID = '1LqB6gIzSXP5tmieYW323At8eYymjfdtnmnV78GINkNY'
_SHEET_NAME = 'capacity-plan'
_RANGE_NAME = '%s!7:10000' % _SHEET_NAME

_BQ_DATASET = 'sheets_to_bq'
_BQ_TABLE = _SHEET_NAME

_TMP_FILENAME = 'tmp.csv'
_SHEET = collections.namedtuple('Sheet', ('name', 'id'))


def _generate_filepath(name):
  """Generate temporary file path."""
  return os.path.join(tempfile.gettempdir(), name)


def _get_credentials_json():
  client = storage.Client()
  bucket = client.bucket(_CREDENTIALS_GCS_BUCKET_NAME)
  blob = bucket.get_blob(_CREDENTIALS_GCS_FILENAME)
  filepath = _generate_filepath(_CREDENTIALS_FILENAME)
  if blob:
    blob.download_to_filename(filepath)


def _check_creds(token_filename):
  """Check if token is available locally and refresh if neccessary."""
  creds = None
  token_filepath = _generate_filepath(token_filename)
  if os.path.exists(token_filepath):
    with open(token_filepath, 'rb') as token:
      creds = pickle.load(token)
  if not creds or not creds.valid:
    if creds and creds.expired:
      creds.refresh(Request())
  return creds


def _fetch_creds(creds_filename):
  """Build credentials from json file."""
  _get_credentials_json()
  creds_filepath = _generate_filepath(creds_filename)
  creds = None
  try:
    creds = service_account.Credentials.from_service_account_file(
        creds_filepath, scopes=_SCOPES)
  except FileNotFoundError as err:
    logging.error(err)
  return creds


def _save_creds(creds, token_filename):
  """Save credentials offline for next run."""
  token_filepath = _generate_filepath(token_filename)
  with open(token_filepath, 'wb') as token:
    pickle.dump(creds, token)


def _sheets_client(creds):
  """Build Sheets service client."""
  service = build(_SHEETS_SERVICE_NAME,
                  _SHEETS_SERVICE_API_VERSION,
                  credentials=creds)
  sheet = service.spreadsheets()  # pylint: disable=no-member
  return sheet


def _fetch_sheet_values(sheets_client,
                        sheet_id=_SPREADSHEET_ID,
                        sheet_range_name=_RANGE_NAME):
  """Fetch values from the given sheet and range."""
  result = sheets_client.values().get(spreadsheetId=sheet_id,
                                      range=sheet_range_name).execute()
  values = result.get('values', [])
  return values


def _write_to_tmp(values, filepath):
  """Write sheet values locally to a tmp file."""
  with open(filepath, 'w') as out_file:
    csv_writer = csv.writer(out_file,
                            quoting=csv.QUOTE_ALL,
                            escapechar='\\',
                            quotechar='"')
    csv_writer.writerows(values)


def _delete_tmp(filepath):
  """Delete locally created tmp file."""
  try:
    os.remove(filepath)
  except FileNotFoundError as err:
    logging.error(err)


def _bq_client(creds):
  """Build Bigquery client."""
  return bigquery.Client(credentials=creds)


def _load_data(bq_client, table, filepath):
  """Load data from tmp CSV file to target bigquery table."""
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.skip_leading_rows = 1
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
  job_config.autodetect = True

  with open(filepath, "rb") as source_file:
    job = bq_client.load_table_from_file(source_file,
                                         table,
                                         job_config=job_config)
  job.result()  # Waits for table load to complete.
  return job.state


def _get_bq_dataset(bq_client, dataset_name=_BQ_DATASET):
  """Check if dataset exists, else create it."""
  dataset = None
  try:
    dataset = bq_client.get_dataset(dataset_name)
  except exceptions.Forbidden as err:
    logging.error(err)
  except exceptions.NotFound as err:
    logging.warning(err)
    logging.info('Creating dataset: %s', dataset_name)
    dataset = bq_client.create_dataset(dataset_name)
  return dataset


def _get_bq_table(bq_client, dataset_name=_BQ_DATASET, table_name=_SHEET_NAME):
  """Check if table exists, else create it."""
  table = None
  table_name = table_name.replace('-', '_')
  full_table_name = '%s.%s' % (dataset_name, table_name)
  try:
    table = bq_client.get_table(full_table_name)
  except exceptions.Forbidden as err:
    logging.error(err)
  except exceptions.NotFound as err:
    logging.warning(err)
    logging.info('Creating table: %s', full_table_name)
    table = bq_client.create_table(full_table_name)
  return table


def _drive_client(creds=None):
  """Build drive service client."""
  return build(_DRIVE_SERVICE_NAME,
               _DRIVE_SERVICE_API_VERSION,
               credentials=creds)


def _shared_sheet_files(drive_client, mime_type=_MIME_TYPE):
  """Fetch the details of files shared with the user."""
  result = drive_client.files().list(q='mimeType="%s"' % mime_type).execute()
  for sheet_file in result['files']:
    sheet = _SHEET(sheet_file['name'], sheet_file['id'])
    yield sheet


def _fetch_and_load_data(sheet, sheets_client, bq_client, timestamp):
  """Fetch data from Google Sheet and load data into BQ."""
  sheet_values = _fetch_sheet_values(sheets_client, sheet_id=sheet.id)
  filepath = _generate_filepath('%s.csv' % sheet.name)
  _write_to_tmp(sheet_values, filepath)

  dataset = _get_bq_dataset(bq_client, dataset_name=_BQ_DATASET)
  table = _get_bq_table(bq_client,
                        dataset_name=dataset.dataset_id,
                        table_name=sheet.name)
  _ = _load_data(bq_client, table, filepath)

  name = '%s_history' % _BQ_DATASET
  history_dataset = _get_bq_dataset(bq_client, dataset_name=name)
  history_table = _get_bq_table(bq_client,
                                dataset_name=history_dataset.dataset_id,
                                table_name='%s_%s' % (sheet.name, timestamp))
  _ = _load_data(bq_client, history_table, filepath)
  _delete_tmp(filepath)


def sheets_to_bq(request):
  """Load data from Google Sheets to BQ table."""
  creds = _check_creds(_TOKEN_FILENAME) or _fetch_creds(_CREDENTIALS_FILENAME)
  if not creds:
    logging.error('Could not fetch credentials')
    if request:
      flask.abort(500)
    else:
      print('Quit')
      return

  drive_client = _drive_client(creds)
  sheets_client = _sheets_client(creds)
  bq_client = _bq_client(creds)
  _ = _get_bq_dataset(bq_client)
  _ = _get_bq_dataset(bq_client, '%s_history' % _BQ_DATASET)

  timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
  shared_sheets = _shared_sheet_files(drive_client)
  for sheet in shared_sheets:
    _fetch_and_load_data(sheet, sheets_client, bq_client, timestamp)

  if creds.token:
    _save_creds(creds, _TOKEN_FILENAME)
  return 'DONE: imported all sheets'


if __name__ == '__main__':
  sheets_to_bq(None)
