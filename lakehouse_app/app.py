from flask import Flask, render_template, request, jsonify
import subprocess
import threading
import queue
import time
import os
import logging
import errno
import re
# Use pty to create a pseudo-terminal for better interactive support
import pty
import select
import fcntl
import termios
import struct
import signal
import json

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("dlt-meta-app.log"),
                              logging.StreamHandler()])
logger = logging.getLogger(__name__)

app = Flask(__name__)
command_queues = {}
response_queues = {}


def run_command(command_id, command, input_queue, output_queue, SHELL_FLG=False):

    # Handle export commands
    if command.startswith('export'):
        try:
            var, value = command.split(' ', 1)[1].split('=', 1)
            os.environ[var] = value
            print(f"env var: {os.environ[var]}")
            output_queue.put(('output', f"Exported {var}={value}\n"))
            output_queue.put(('exit', 0))
        except Exception as e:
            output_queue.put(('error', str(e)))
            output_queue.put(('exit', 1))
        return
    # Handle cd commands
    if command.startswith('cd'):
        try:
            path = command.split(' ', 1)[1]
            os.chdir(path)
            output_queue.put(('output', f"Changed directory to {os.getcwd()}\n"))
            output_queue.put(('exit', 0))
        except Exception as e:
            output_queue.put(('error', str(e)))
            output_queue.put(('exit', 1))
        return

    # If command is a Python script, ensure unbuffered output
    if command.startswith('python'):
        command = command.replace('python', 'python -u', 1)
    # Create a pseudo-terminal
    master, slave = pty.openpty()
    # Set the terminal size
    term_size = struct.pack('HHHH', 24, 80, 0, 0)
    fcntl.ioctl(slave, termios.TIOCSWINSZ, term_size)
    # Start the process
    process = subprocess.Popen(
        ["bash", "-c", command],
        shell=SHELL_FLG,
        stdin=slave,
        stdout=slave,
        stderr=slave,
        preexec_fn=os.setsid,
        text=False,
        bufsize=0,
        env=os.environ.copy()
    )
    # Close the slave fd, as the child process has it
    os.close(slave)
    # Set master to non-blocking mode
    fl = fcntl.fcntl(master, fcntl.F_GETFL)
    fcntl.fcntl(master, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    # Variables for tracking state
    waiting_for_input = False
    buffer = ""

    # Create events for signaling
    stop_event = threading.Event()
    last_prompt = None
    last_prompt_time = 0

    def output_reader():
        nonlocal buffer, waiting_for_input, last_prompt, last_prompt_time
        while not stop_event.is_set() and process.poll() is None:
            try:
                # Check if there's data to read
                r, _, _ = select.select([master], [], [], 0.1)
                if master in r:
                    # Read data
                    data = os.read(master, 1024)
                    if not data:
                        break
                    # Decode the data
                    try:
                        text = data.decode('utf-8')
                    except UnicodeDecodeError:
                        text = data.decode('latin-1')
                    # Add to output queue
                    output_queue.put(('output', text))
                    # Add to buffer for prompt detection
                    buffer += text
                    # Check for prompts in the buffer
                    lines = buffer.splitlines(True)
                    buffer = ""
                    for line in lines:
                        if line.endswith('\n'):
                            buffer = ""
                        else:
                            buffer += line
                        # Check if line looks like a prompt
                        if (('?' in line or ':' in line
                             or line.strip().endswith('>')
                             or '[y/n]' in line
                             or 'input' in line.lower()
                             or 'select' in line.lower()
                             or 'choose' in line.lower()
                             or 'continue' in line.lower()
                             or 'press' in line.lower()) and not line.strip().endswith('\\')):
                            # output_queue.put(('prompt', line))
                            # waiting_for_input = True

                            # Deduplicate prompts
                            current_time = time.time()
                            if last_prompt != line or current_time - last_prompt_time > 1.0:
                                output_queue.put(('prompt', line))
                                waiting_for_input = True
                                last_prompt = line
                                last_prompt_time = current_time
            except (OSError, IOError) as e:
                if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
                    output_queue.put(('error', f"Output error: {str(e)}"))
                    break
            except Exception as e:
                output_queue.put(('error', f"Output error: {str(e)}"))
                break
            time.sleep(0.01)
    # Start output reader thread
    output_thread = threading.Thread(target=output_reader)
    output_thread.daemon = True
    output_thread.start()
    # Main loop for handling input
    try:
        while process.poll() is None:
            try:
                # Check if we have user input to send
                if not input_queue.empty():
                    user_input = input_queue.get()
                    # Add newline and encode
                    input_data = (user_input + '\n').encode('utf-8')
                    # Write to the master fd
                    os.write(master, input_data)
                    # Add to output queue
                    output_queue.put(('input', user_input))
                    # Reset state
                    waiting_for_input = False
                    last_prompt = None
                # If we're waiting for input but none is available, sleep briefly
                elif waiting_for_input:
                    time.sleep(0.1)
                # Otherwise just wait a bit
                else:
                    time.sleep(0.1)
            except Exception as e:
                output_queue.put(('error', f"Input error: {str(e)}"))
                time.sleep(0.1)
    except KeyboardInterrupt:
        # Handle keyboard interrupt
        pass
    finally:
        # Signal the output thread to stop
        stop_event.set()
        # Try to terminate the process gracefully
        try:
            if process.poll() is None:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=1)
        except Exception:
            # Force kill if necessary
            try:
                if process.poll() is None:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except Exception:
                pass
        # Close the master fd
        try:
            os.close(master)
        except Exception:
            pass
        # Wait for output thread to finish
        output_thread.join(timeout=1)
        # Process has ended
        exit_code = process.poll() if process.poll() is not None else -1
        output_queue.put(('exit', exit_code))


@app.route('/')
def index():
    return render_template('landingPage.html')


@app.route('/start_command', methods=['POST'])
def start_command():
    data = request.json
    command = data.get('command')

    if command == 'setup':
        try:
            current_directory = os.getcwd()
            print(f"Current directory: {current_directory}")
        except FileNotFoundError:
            print("The current working directory is no longer accessible.")
            # Optionally, set a default directory
            os.chdir("/")  # Change to root directory
            current_directory = os.getcwd()

        command_id = None
        # Chain commands with && to ensure they run in sequence
        if 'PYTHONPATH' not in os.environ or not os.path.isdir(os.environ.get('PYTHONPATH', '')):
            commands = [
                "pip install databricks-cli",
                # "git clone https://github.com/databrickslabs/dlt-meta.git",
                "git clone https://github.com/dattawalake/dlt-meta.git",
                f"python -m venv {current_directory}/dlt-meta/.venv",
                f"export HOME={current_directory}",
                "cd dlt-meta",
                "source .venv/bin/activate",
                f"export PYTHONPATH={current_directory}/dlt-meta/",
                "pwd",
                "pip install databricks-sdk",
            ]
            print("Start setting up dlt-meta environment")
            for c in commands:
                try:
                    command_id = str(time.time())

                    input_queue = queue.Queue()
                    output_queue = queue.Queue()

                    command_queues[command_id] = input_queue
                    response_queues[command_id] = output_queue
                    run_command(command_id, c, input_queue, output_queue, False)
                    print(f"complete setup command : {c}")
                except Exception as e:
                    logger.error(f"Error starting command: {str(e)}")
                    print(f"Error starting command: {str(e)}")
            print("Completed setting up dlt-meta environment")

    else:
        command_id = str(time.time())
        input_queue = queue.Queue()
        output_queue = queue.Queue()
        command_queues[command_id] = input_queue
        response_queues[command_id] = output_queue
        thread = threading.Thread(target=run_command, args=(command_id, command, input_queue, output_queue))
        thread.daemon = True
        thread.start()
    return jsonify({'command_id': command_id})


@app.route('/send_input', methods=['POST'])
def send_input():
    data = request.json
    command_id = data.get('command_id')
    user_input = data.get('input')
    if command_id in command_queues:
        command_queues[command_id].put(user_input)
        return jsonify({'status': 'success'})
    else:
        return jsonify({'status': 'error', 'message': 'Command not found'})


@app.route('/get_output', methods=['GET'])
def get_output():
    command_id = request.args.get('command_id')
    if command_id in response_queues:
        output_queue = response_queues[command_id]
        result = []
        while not output_queue.empty():
            output_type, content = output_queue.get()
            result.append({'type': output_type, 'content': content})
        return jsonify({'status': 'success', 'output': result})
    else:
        return jsonify({'status': 'error', 'message': 'Command not found'})


@app.route('/cleanup', methods=['POST'])
def cleanup():
    data = request.json
    command_id = data.get('command_id')
    if command_id in command_queues:
        del command_queues[command_id]
    if command_id in response_queues:
        del response_queues[command_id]
    return jsonify({'status': 'success'})


@app.route('/onboarding', methods=['POST'])
def handle_onboard_form():

    print(f"onboard details: {request.form}")
    current_directory = os.environ['PYTHONPATH']  # os.getcwd()

    # Create JSON object from form data
    json_data = {
        "unity_catalog_enabled": "1" if request.form.get('unity_catalog_enabled') == "1" else "0",
        "unity_catalog_name": request.form.get('unity_catalog_name', ''),
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "onboarding_file_path": request.form.get('onboarding_file_path', 'demo/conf/onboarding.template'),
        "local_directory": request.form.get('local_directory', '/app/python/source_code/dlt-meta/demo/'),
        "dlt_meta_schema": request.form.get('dlt_meta_schema',
                                            'dlt_meta_dataflowspecs_4e6c360d3e5c4b5ca6687fec8ffe2e14'),
        "bronze_schema": request.form.get('bronze_schema', 'dltmeta_bronze_9c1aa383b36a49198d3e99d25f7180a4'),
        "silver_schema": request.form.get('silver_schema', 'dltmeta_silver_7b4e981029b843c799bf61a0a121b3ca'),
        "dlt_meta_layer": request.form.get('dlt_meta_layer', '1'),
        "bronze_table": request.form.get('bronze_table', 'bronze_dataflowspec'),
        "overwrite": "1" if request.form.get('overwrite') == "1" else "0",
        "version": request.form.get('version', 'v1'),
        "environment": request.form.get('environment', 'prod'),
        "author": request.form.get('author', 'app-40zbx9 meta-dlt'),
        "update_paths": "1" if request.form.get('update_paths') == "1" else "0",
        "command": "onboard_ui",
        "flags": {"log_level": "info"},
    }

    json_string = json.dumps(json_data)
    result = subprocess.run(f"python {current_directory}src/cli.py '{json_string}'",
                            shell=True,
                            capture_output=True,
                            text=True
                            )
    return extract_command_output(result)


@app.route('/deploy', methods=['POST'])
def handle_deploy_form():
    # Create JSON object from form data
    print(f"deploy details: {request.form}")
    current_directory = os.environ['PYTHONPATH']  # os.getcwd()

    json_data = {
        "uc_enabled": "1" if request.form.get('uc_enabled') == "1" else "0",
        "uc_catalog_name": request.form.get('uc_catalog_name', ''),
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "layer": request.form.get('deploylayer', 'bronze'),
        "pipeline_name": request.form.get('pipeline_name', 'dlt_meta_pipeline'),
        "dlt_target_schema": request.form.get("dlt_target_schema"),
        "command": "deploy_ui",
        "flags": {"log_level": "info"},
        "onboard_bronze_group": request.form.get("onboard_bronze_group"),
        "onboard_silver_group": request.form.get("onboard_silver_group"),
        "dlt_meta_schema": request.form.get("spc_schema_name"),
        "bronze_dataflowspec_table": request.form.get("bronze_dataflowspec_table"),
        "dataflowspec_silver_table": request.form.get("silver_dataflowspec_table"),
    }

    json_string = json.dumps(json_data)
    result = subprocess.run(f"python {current_directory}/src/cli.py '{json_string}'",
                            shell=True,
                            capture_output=True,
                            text=True
                            )
    return extract_command_output(result)


@app.route('/rundemo', methods=['POST'])
def run_demo():
    code_to_run = request.json.get('demo_name', '')
    print(f"processing demo for :{request.json}")
    current_directory = os.environ['PYTHONPATH']  # os.getcwd()
    demo_dict = {"demo_cloudfiles": "demo/launch_af_cloudfiles_demo.py",
                 "demo_acf": "demo/launch_acfs_demo.py",
                 "demo_silverfanout": "demo/launch_silver_fanout_demo.py",
                 "demo_dias": "demo/launch_dais_demo.py"
                 }
    demo_file = demo_dict.get(code_to_run, None)
    uc_name = request.json.get('uc_name', '')
    result = subprocess.run(f"python {current_directory}/{demo_file} --uc_catalog_name {uc_name} --profile DEFAULT",
                            shell=True,
                            capture_output=True,
                            text=True
                            )
    return extract_command_output(result)


def extract_command_output(result):
    stdout = result.stdout
    job_id_match = re.search(r"job_id=(\d+) | pipeline=(\d+)", stdout)
    url_match = re.search(r"url=(https?://[^\s]+)", stdout)

    job_id = job_id_match.group(1) or job_id_match.group(2) if job_id_match else None
    job_url = url_match.group(1) if url_match else None

    if job_url:
        modal_html = {'title': 'Job Created Successfully',
                      'job_id': job_id,
                      'job_url': job_url
                      }
    else:
        modal_html = None
    # Return the response as JSON
    return jsonify({
        'modal_content': modal_html,
        'stdout': result.stdout,
        'stderr': result.stderr,
        'returncode': result.returncode
    })


if __name__ == '__main__':
    app.run(debug=True)
