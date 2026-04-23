## S45 DRHP Agent (take-home)

This repo contains:
- **Dummy datasets** for `SH-7` and `PAS-3` packets under `data/sample_dataset/`
- A basic **pipeline runner** in `main.py` that scans SH-7 packets and attempts structured extraction
- A draft output table written to `output/authorised_capital_changes.md`

### Setup

Create a virtualenv and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Create your env file:

```bash
cp .env.example .env
```

Then set `OPENAI_API_KEY` in `.env`.

### Run

```bash
python3 main.py
```

Overrides:

```bash
DATA_DIR="data/sample_dataset/sh7" OUTPUT_PATH="output/authorised_capital_changes.md" python3 main.py
```

