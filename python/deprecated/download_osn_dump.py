import os
import subprocess

def execute_shell_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.decode().strip(), stderr.decode().strip()

# Execute curl command
curl_command = 'curl -O https://dl.min.io/client/mc/release/linux-amd64/mc'
execute_shell_command(curl_command)

# Execute chmod command
chmod_command = 'chmod +x mc'
execute_shell_command(chmod_command)

# Execute mc alias command
mc_alias_command = './mc alias set opensky https://s3.opensky-network.org $OSN_USERNAME $OSN_KEY'
execute_shell_command(mc_alias_command)

# Get the list of already downloaded files
downloaded_files = os.listdir('data/ec-datadump/')

# Execute mc find command to list files
find_command = './mc find opensky/ec-datadump/ --path "*/states_*.parquet"'
stdout, _ = execute_shell_command(find_command)
files_to_download = stdout.split('\n')

# Execute mc cp command for each file not yet downloaded
for file in files_to_download:
    if file and file.split('/')[-1] not in downloaded_files:
        cp_command = f'./mc cp "{file}" data/ec-datadump/'
        print(f"Executing {cp_command}")
        out, err = execute_shell_command(cp_command)
        print(f"Stdout: [{out}].")
        print(f"Stderr: [{err}].")
        print("="*20)
        print()