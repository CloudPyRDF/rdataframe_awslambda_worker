import ROOT
import base64
import boto3
import cloudpickle as pickle
import json
import os
import time
from multiprocessing import Process, Pipe

from Inspector import Inspector

bucket = os.getenv('bucket')
debug_command = os.getenv('debug_command', '')
return_after_debug = os.getenv('return_after_debug', 'False')

krb5ccname = os.getenv('KRB5CCNAME', '/tmp/certs')
monitor = (os.getenv('monitor', 'False') == 'True')
results_fname = os.getenv('results_fname', 'results.txt')


def the_monitor(pipe,rangeid):
    rangeid = hash(rangeid)
    def network_measurement():
        memInfo = ""
        with open('/proc/net/dev', 'r') as file:
            memInfo = file.read()
        my_dict = {}
        for line in memInfo.split('\n')[2:-1]:
            splitted = line.split(':')
            my_dict[splitted[0].strip()] = int(
                splitted[1].lstrip().split(maxsplit=1)[0]
            )
        return my_dict

    def inspect_me(rangeid):
        inspector = Inspector()
        inspector.inspectAll()
        inspector.addAttribute("network_rx_bytes", network_measurement())
        inspector.addAttribute("taskID", rangeid)

        return inspector.finish()

    with open("/tmp/readings.txt","w+") as f:    
        f.write(" ")

    while True:
        os.nice(0)
        with open("/tmp/readings.txt","a") as f:
            f.write(json.dumps(inspect_me(rangeid)))
            f.write("\n")
        # pipe.send(inspect_me())
        time.sleep(1)

class run_monitoring_thread:
    """ Context manager for running monitoring
    """
    def __init__(self, rdf_range):
        self.rdf_range = rdf_range
        self.thread = None

    def __enter__(self):
        if not monitor:
            return

        try:
            os.remove("/tmp/readings.txt")
        except Exception:
            pass

        pipe_in, pipe_out = Pipe()
        self.thread = Process(target=the_monitor, args=(pipe_in, self.rdf_range))
        self.thread.start()
        print('monitoring started!')

    def __exit__(self, exception_type, exception_value, traceback):
        if not monitor:
            return
        print('monitoring stopping!')
        self.thread.terminate()
        self.thread.join()
        print('monitoring finished!')

def get_monitoring_results():
    measurement_results = []
    if not monitor:
        return measurement_results
    with open("/tmp/readings.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            measurement_results.append(json.loads(line))

def run_mapper(mapper, rdf_range) -> dict:
    s3_client = boto3.client('s3')

    with run_monitoring_thread(rdf_range):
        try:
            hist = mapper(range)
        except Exception as exception:
            return {
                'statusCode': 500,
                'errorType': json.dumps(type(exception).__name__),
                'errorMessage': json.dumps(str(exception)),
            }

        with open('/tmp/out.pickle', 'wb') as handle:
            pickle.dump(hist, handle)

        filename = f'partial_{range.id}_{str(int(time.time() * 1000.0))}.pickle'
        s3_client.upload_file(f'/tmp/out.pickle', bucket, filename)

    return {
        'statusCode': 200,
        'body': json.dumps(get_monitoring_results()),
        'filename': json.dumps(filename)
    }

def handle_debug(debug_command: str | None) -> dict | None:
    if debug_command is None:
        return None

    if return_after_debug:
        return {
            'statusCode': 500,
            'command': debug_command,
            'command_output': os.popen(f'{debug_command}').read()
        }
    else:
        print(debug_command)
        return None

def write_cert(cert_file: bytes):
    with open(f'{krb5ccname}', "wb") as handle:
        handle.write(cert_file)
    
def declare_headers(headers: list):
    for header_name, header_content in headers:
        header_path = '/tmp/' + header_name
        with open(header_path, 'w') as f:
            f.write(header_content)
        print(header_name)
        try:
            ROOT.gInterpreter.Declare(f'#include "{header_path}"')
        except Exception:
            print(f'Could not declare header {header_name}')


def lambda_handler(event, context):

    debug_info = handle_debug(event.get('debug_command'))
    if debug_info is not None:
        return debug_info
    
    print('event', event)

    rdf_range = pickle.loads(base64.b64decode(event['range'][2:-1]))
    mapper    = pickle.loads(base64.b64decode(event['script'][2:-1]))
    headers   = pickle.loads(base64.b64decode(event['headers'][2:-1]))
    cert_file = base64.b64decode(event['cert'][2:-1])

    print(rdf_range)
   
    write_cert(cert_file)
    declare_headers(headers)

    response = run_mapper(mapper, rdf_range)

    return response
