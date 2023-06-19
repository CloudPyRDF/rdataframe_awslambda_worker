import ROOT
import base64
import boto3
import cloudpickle as pickle
import json
import os
import logging
import time

from monitor import CPUAndNetMonitor, EmptyMonitor, monitoring_thread


logging.basicConfig(level=logging.DEBUG)
bucket = os.getenv('bucket')
debug_command = os.getenv('debug_command', '')
return_after_debug = os.getenv('return_after_debug', 'False')

krb5ccname = os.getenv('KRB5CCNAME', '/tmp/certs')
monitoring_on = (os.getenv('monitor', 'False') == 'True')


def lambda_handler(event, context):

    debug_info = handle_debug(event.get('debug_command'))
    if debug_info is not None:
        return debug_info
    
    logging.info(f'event {event}')

    rdf_range = pickle.loads(base64.b64decode(event['range']))
    mapper    = pickle.loads(base64.b64decode(event['script']))
    headers   = pickle.loads(base64.b64decode(event['headers']))
    cert_file = base64.b64decode(event['cert'])
    s3_access_key = pickle.loads(base64.b64decode(event['S3_ACCESS_KEY']))
    s3_secret_key = pickle.loads(base64.b64decode(event['S3_SECRET_KEY']))
    os.environ['S3_ACCESS_KEY'] = s3_access_key
    os.environ['S3_SECRET_KEY'] = s3_secret_key

    logging.info(rdf_range)
   
    write_cert(cert_file)
    declare_headers(headers)

    return run(mapper, rdf_range)

def handle_debug(debug_command: str | None) -> dict | None:
    if debug_command is None:
        return None

    if return_after_debug:
        return {
            'statusCode': 500,
            'command': debug_command,
            'command_output': os.popen(f'{debug_command}').read()
        }

    logging.info(debug_command)
    return None

def write_cert(cert_file: bytes):
    with open(f'{krb5ccname}', "wb") as handle:
        handle.write(cert_file)

def declare_headers(headers: list):
    for header_name, header_content in headers:
        header_path = '/tmp/' + header_name
        with open(header_path, 'w') as f:
            f.write(header_content)
        logging.info(f'Declaring header: {header_name}')
        try:
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
        except Exception:
            logging.error(f'Could not declare header {header_name}')

def run(mapper, rdf_range) -> dict:
    monitor = get_monitor(rdf_range)

    with monitoring_thread(monitor):
        try:
            hist = mapper(rdf_range)
        except Exception as exception:
            return {
                'statusCode': 500,
                'errorType': json.dumps(type(exception).__name__),
                'errorMessage': json.dumps(str(exception)),
            }

        filename = serialize_and_upload_to_s3(hist, rdf_range.id)

    return {
        'statusCode': 200,
        'body': json.dumps(monitor.get_monitoring_results()),
        'filename': json.dumps(filename)
    }

def get_monitor(rdf_range):
    if monitoring_on:
        return CPUAndNetMonitor(rdf_range.id)
    else:
        return EmptyMonitor()

def serialize_and_upload_to_s3(hist, rangeid):
    pickled_hist = pickle.dumps(hist)
    filename = get_unique_filename(rangeid)
    upload_result_to_s3(pickled_hist, filename)
    return filename

def get_unique_filename(range_id):
    timestamp = int(time.time() * 1000.0)
    return f'output/partial_{range_id}_{timestamp}.pickle'
    
def upload_result_to_s3(obj: bytes, filename: str):
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=obj, Bucket=bucket, Key=filename)
