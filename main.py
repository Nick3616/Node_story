import csv
import paramiko
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_command(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.read().decode(), stderr.read().decode()

def execute_commands(ip, username, password, log_file):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username=username, password=password)
        print(f"Connected to {ip}")

        commands = [
            "sudo apt update && sudo apt install curl make unzip clang pkg-config lz4 libssl-dev build-essential git jq ncdu bsdmainutils htop -y < '/dev/null'",
            "cd $HOME && VERSION=1.23.0 && wget -O go.tar.gz https://go.dev/dl/go$VERSION.linux-amd64.tar.gz && sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go.tar.gz && rm go.tar.gz",
            "echo 'export GOROOT=/usr/local/go' >> $HOME/.bash_profile && echo 'export GOPATH=$HOME/go' >> $HOME/.bash_profile && echo 'export GO111MODULE=on' >> $HOME/.bash_profile && echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> $HOME/.bash_profile && source $HOME/.bash_profile",
            "cd $HOME && rm -rf story",
            "wget -O story-linux-amd64-0.9.11-2a25df1.tar.gz https://story-geth-binaries.s3.us-west-1.amazonaws.com/story-public/story-linux-amd64-0.9.11-2a25df1.tar.gz",
            "tar xvf story-linux-amd64-0.9.11-2a25df1.tar.gz",
            "sudo chmod +x story-linux-amd64-0.9.11-2a25df1/story && sudo mv story-linux-amd64-0.9.11-2a25df1/story /usr/local/bin/",
            "story version",
            "cd $HOME && rm -rf story-geth",
            "wget -O geth-linux-amd64-0.9.2-ea9f0d2.tar.gz https://story-geth-binaries.s3.us-west-1.amazonaws.com/geth-public/geth-linux-amd64-0.9.2-ea9f0d2.tar.gz",
            "tar xvf geth-linux-amd64-0.9.2-ea9f0d2.tar.gz",
            "sudo chmod +x geth-linux-amd64-0.9.2-ea9f0d2/geth && sudo mv geth-linux-amd64-0.9.2-ea9f0d2/geth /usr/local/bin/story-geth",
            "story init --network iliad",
            "sleep 1",
            "story validator export --export-evm-key --evm-key-path $HOME/.story/.env",
            "story validator export --export-evm-key >>$HOME/.story/story/config/wallet.txt",
            "cat $HOME/.story/.env >>$HOME/.story/story/config/wallet.txt",
            """
sudo tee /etc/systemd/system/story-geth.service > /dev/null <<EOF  
[Unit]
Description=Story execution daemon
After=network-online.target

[Service]
User=$USER
ExecStart=/usr/local/bin/story-geth --iliad --syncmode full
Restart=always
RestartSec=3
LimitNOFILE=infinity
LimitNPROC=infinity

[Install]
WantedBy=multi-user.target
EOF
            """,
            """
sudo tee /etc/systemd/system/story.service > /dev/null <<EOF  
[Unit]
Description=Story consensus daemon
After=network-online.target

[Service]
User=$USER
WorkingDirectory=$HOME/.story/story
ExecStart=/usr/local/bin/story run
Restart=always
RestartSec=3
LimitNOFILE=infinity
LimitNPROC=infinity

[Install]
WantedBy=multi-user.target
EOF
            """,
            "sudo tee /etc/systemd/journald.conf > /dev/null <<EOF\nStorage=persistent\nEOF",
            "PORT=335 && if ss -tulpen | awk '{print $5}' | grep -q ':26656$'; then sed -i -e 's|:26656\"|:${PORT}56\"|g' $HOME/.story/story/config/config.toml; fi",
            "if ss -tulpen | awk '{print $5}' | grep -q ':26657$'; then sed -i -e 's|:26657\"|:${PORT}57\"|' $HOME/.story/story/config/config.toml; fi",
            "if ss -tulpen | awk '{print $5}' | grep -q ':26658$'; then sed -i -e 's|:26658\"|:${PORT}58\"|' $HOME/.story/story/config/config.toml; fi",
            "if ss -tulpen | awk '{print $5}' | grep -q ':1317$'; then sed -i -e 's|:1317\"|:${PORT}17\"|' $HOME/.story/story/config/story.toml; fi",
            "sudo systemctl restart systemd-journald",
            "sudo systemctl daemon-reload",
            "sudo systemctl enable story",
            "sudo systemctl restart story",
            "sudo systemctl enable story-geth",
            "sudo systemctl restart story-geth",
            "sleep 5",
            "service story status",
            "service story-geth status"
        ]

        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"Log for {ip} - {datetime.now()}\n\n")
            for index, command in enumerate(commands):
                print(f"Executing on {ip}: {command}")
                stdout, stderr = run_command(ssh, command)
                
                if index >= 12:
                    f.write(f"Executing: {command}\n")
                    f.write(f"Output: {stdout}\n")
                    if stderr:
                        f.write(f"Error: {stderr}\n")
                    f.write("\n")

        ssh.close()

    except Exception as e:
        error_msg = f"Error connecting to {ip}: {str(e)}"
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(f"{error_msg}\n")

def process_servers(servers, max_workers=60):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(execute_commands, server['ip'], server['name'], server['passed'], f"logs/{server['ip']}.log") for server in servers]
        for future in as_completed(futures):
            future.result()

servers = []
with open('server.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    servers = list(csv_reader)

process_servers(servers)

print("All servers processed.")
