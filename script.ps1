# Install Chocolatey (package manager for Windows)
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install applications using Chocolatey
choco install googlechrome -y
choco install vscode -y
choco install python -y
choco install metatrader5 -y

# Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# Configure firewall rules for port 5000
netsh advfirewall firewall add rule name="Flask Server" protocol=TCP dir=in localport=5000 action=allow
