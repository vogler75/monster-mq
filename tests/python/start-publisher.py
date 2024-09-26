import subprocess
import sys
import threading
import time
import paho.mqtt.client as mqtt
import uuid

AMOUNT_CLIENTS = 500
AMOUNT_NRS = AMOUNT_CLIENTS

# MQTT settings
MQTT_BROKER = "scada"
MQTT_PORT = 1883
MQTT_TOPIC = "monitor/publisher"

# The command to run your Python program (replace 'your_program.py' with your actual script)
base_command = [sys.executable, "test-publisher.py"]

# Loop to start the program n times
#hosts = [["localhost", 1883]]
hosts = [["192.168.1.3", 1883]]
#hosts = [["192.168.1.31", 1883], ["192.168.1.32", 1883], ["192.168.1.33", 1883]]
#hosts = [["192.168.1.30", 1883], ["192.168.1.30", 1884]] #, ["192.168.1.30", 1885]]

hosts_idx = 0
nrs = [str(i) for i in range(1, AMOUNT_NRS+1)]

# Connect to the MQTT broker
client = mqtt.Client(str(uuid.uuid4()), clean_session=True)
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_start()

while not client.is_connected():
    print("Wait for MQTT Connect.")
    time.sleep(1)


# Function to continuously read output
def read_output(process, instance_number):
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            #print(f"Instance {instance_number}: {output.strip()}")
            topic = f"{MQTT_TOPIC}/instance_{instance_number}"
            client.publish(topic, output.strip())
    # Capture any remaining stderr after process ends
    stderr = process.stderr.read()
    if stderr:
        print(f"Errors from instance {instance_number}: {stderr.strip()}")


nrs_idx = 0
processes = []
threads = []
for i in range(AMOUNT_CLIENTS):
    #time.sleep(0.100)

    # Start the program in the background
    command = base_command + ["--host", hosts[hosts_idx][0], "--port", str(hosts[hosts_idx][1]), "--nr", nrs[nrs_idx]]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True)
    processes.append(process)

    thread = threading.Thread(target=read_output, args=(process, i + 1))
    thread.start()
    threads.append(thread)

    print(f"Started instance {i + 1} {command}")
    hosts_idx = hosts_idx + 1 if hosts_idx + 1 < len(hosts) else 0
    nrs_idx = nrs_idx + 1 if nrs_idx + 1 < len(nrs) else 0


# Wait for all threads to complete
#for thread in threads:
#    thread.join()

while True:
    time.sleep(1)
