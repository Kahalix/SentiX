#!/usr/bin/env bash
set -e  # zakończ, jeśli pojawi się błąd

# Wykryj system operacyjny
OS="$(uname -s)"

# Sprawdź, czy requirements.txt istnieje
if [ ! -f requirements.txt ]; then
    echo "❌ Brak pliku requirements.txt w tym folderze!"
    exit 1
fi

# Usuń stare środowisko jeśli istnieje
if [ -d "venv" ]; then
    echo "🗑 Usuwam stare środowisko venv..."
    rm -rf venv
fi

# Wybór polecenia python (Linux zwykle python3, Windows często python)
if command -v python3 &>/dev/null; then
    PYTHON=python3
else
    PYTHON=python
fi

echo "🔹 Tworzę nowe środowisko przy użyciu: $PYTHON"
$PYTHON -m venv venv

# Aktywacja środowiska
if [[ "$OS" == "Linux" || "$OS" == "Darwin" ]]; then
    # Linux / macOS
    source venv/bin/activate
else
    # Windows (Git Bash / WSL)
    source venv/Scripts/activate
fi

echo "📦 Instaluję paczki..."

# Aktualizacja pip
python -m pip install --upgrade pip

# Instalacja paczek
pip install -r requirements.txt

echo "✅ Świeże środowisko gotowe i aktywne!"
