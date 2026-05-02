Set-Location "C:\Models\CODE\2026\Planeorama"

& ".\.venv\Scripts\python.exe" fetch_flights.py

git add data/
$staged = git diff --staged --name-only
if ($staged) {
    git commit -m "data: $(Get-Date -Format 'yyyy-MM-dd') flight update"
    git push
}
