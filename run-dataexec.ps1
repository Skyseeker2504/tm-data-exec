# --- run-dataexec.ps1 (no venv) ---
$ErrorActionPreference = "Stop"

# 1) Go to the API folder
Set-Location -Path "D:\vscode\tm\data-exec"

# 2) Ensure required packages exist (fastapi, uvicorn at minimum)
# If you already installed, this is a no-op; otherwise it installs/updates cleanly.
$req = @(
  "fastapi==0.112.0",
  "uvicorn==0.30.3",
  "duckdb==1.0.0",
  "pandas==2.2.2",
  "openpyxl==3.1.5",
  "python-multipart==0.0.9",
  "python-pptx==0.6.23"
)
pip install $req -q

# 3) Sanity checks
if (-not (Test-Path ".\app.py")) {
  Write-Error "Couldn't find app.py in D:\vscode\tm\data-exec. Make sure your FastAPI file is named app.py and contains 'app = FastAPI()'."
}

# 4) Run Uvicorn from the correct folder
# Using --app-dir ensures correct module resolution even if launched elsewhere.
python -m uvicorn app:app --host 127.0.0.1 --port 8008 --reload --app-dir "D:\vscode\tm\data-exec"
