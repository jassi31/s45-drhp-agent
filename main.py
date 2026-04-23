import logging
import os
from typing import Iterable, List, Literal

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


class AuthorisedCapitalChange(BaseModel):
    """
    Structured extraction for the S45 Task-1 capital structure table.

    All string fields must be filled with 'NOT CONFIRMED' when the value
    cannot be proven from the provided document text.
    """

    meeting_date: str = Field(
        description="Meeting date in the document, or 'NOT CONFIRMED'.",
    )
    meeting_type: Literal["AGM", "EGM", "NOT CONFIRMED"] = Field(
        description="AGM or EGM if explicitly stated; otherwise 'NOT CONFIRMED'.",
    )
    old_capital_breakdown: str = Field(
        description=(
            "Old authorised capital breakdown (e.g., '1,00,000 divided into 10,000 "
            "Equity Shares of 10 each'), or 'NOT CONFIRMED'."
        ),
    )
    new_capital_breakdown: str = Field(
        description="New authorised capital breakdown, or 'NOT CONFIRMED'.",
    )
    source_documents: List[str] = Field(
        description=(
            "List of source document names referenced in the provided text. "
            "Only include names that appear in the input (e.g., 'SH7', 'MOA.md')."
        ),
        default_factory=list,
    )
    is_valid_sh7_event: bool = Field(
        description=(
            "True only if the document indicates an authorised share capital change "
            "(SH-7 / MOA clause V alteration). False for PAS-3 allotments or unrelated docs."
        )
    )


def extract_capital_data(document_text: str) -> AuthorisedCapitalChange:
    """
    Extract authorised share capital change fields using OpenAI Structured Outputs.
    """

    client = OpenAI()

    system_prompt = """
You are a meticulous financial auditor and compliance reviewer for Indian corporate filings.
Your job is to extract ONLY what is explicitly supported by the provided document text.

CRITICAL RULES (NO EXCEPTIONS):
1) Do NOT guess. Do NOT infer. Do NOT fill missing values with “best efforts”.
2) If a required field is not explicitly present in the text, set it to exactly: NOT CONFIRMED
3) PAS-3 (Return of Allotment) documents describe ALLOTMENT / ISSUE of shares and do NOT change
   the company’s AUTHORISED SHARE CAPITAL. If the text looks like PAS-3 or allotment-only,
   you MUST set is_valid_sh7_event = false.
4) Only set is_valid_sh7_event = true when the text clearly indicates an AUTHORISED SHARE CAPITAL
   CHANGE (typically SH-7: “Notice to Registrar of any alteration of share capital”, or resolutions
   altering MOA Clause V / increasing authorised share capital).
5) meeting_type must be AGM or EGM ONLY if the text explicitly says “AGM” or “EGM” (or the full
   phrase “Annual General Meeting” / “Extra Ordinary General Meeting”). Otherwise: NOT CONFIRMED.
6) old_capital_breakdown and new_capital_breakdown must be consistent with the text and should be
   written as a single human-readable string in the format:
   "<amount> divided into <number> Equity Shares of <face value> each"
   If any component is missing, write NOT CONFIRMED for the entire breakdown field.
7) source_documents must list only document names/types that are explicitly mentioned in the text
   (examples: "SH-7", "Form SH-7", "PAS-3", "MOA", "Memorandum of Association", "Board Resolution",
   or explicit filenames like "MOA.md"). Do not invent filenames.

OUTPUT FORMAT:
- You MUST output a single JSON object that matches the provided schema exactly.
""".strip()

    completion = client.beta.chat.completions.parse(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Extract authorised capital change data from the following document text:\n\n"
                    "----- BEGIN DOCUMENT -----\n"
                    f"{document_text}\n"
                    "----- END DOCUMENT -----\n"
                ),
            },
        ],
        response_format=AuthorisedCapitalChange,
    )

    parsed = completion.choices[0].message.parsed
    if parsed is None:
        raise RuntimeError("Structured output parse returned no result.")
    return parsed


def iter_case_dirs(root_dir: str) -> Iterable[str]:
    for entry in sorted(os.listdir(root_dir)):
        case_dir = os.path.join(root_dir, entry)
        if os.path.isdir(case_dir):
            yield case_dir


def read_packet_text(case_dir: str) -> str:
    """
    Read all .md files in a case folder and concatenate into one text payload.
    We include filenames as headers to help the model populate `source_documents`
    without inventing names.
    """
    parts: List[str] = []
    for filename in sorted(os.listdir(case_dir)):
        if not filename.lower().endswith(".md"):
            continue
        path = os.path.join(case_dir, filename)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        parts.append(f"===== SOURCE DOCUMENT: {filename} =====\n{content}".strip())
    return "\n\n".join(parts).strip()


def process_all_events(data_directory: str) -> List[AuthorisedCapitalChange]:
    logger = logging.getLogger("pipeline")
    events: List[AuthorisedCapitalChange] = []

    for case_dir in iter_case_dirs(data_directory):
        case_name = os.path.basename(case_dir)
        logger.info("Processing case folder: %s", case_name)

        packet_text = read_packet_text(case_dir)
        if not packet_text:
            logger.warning("Empty packet text for case %s", case_name)
            continue

        try:
            extracted = extract_capital_data(packet_text)
        except Exception:
            logger.exception("Extraction failed for case %s", case_name)
            continue

        if extracted.is_valid_sh7_event:
            events.append(extracted)
            logger.info("Valid SH-7 event extracted for %s", case_name)
        else:
            logger.info("Not a valid SH-7 authorised-capital event for %s", case_name)

    return events


def generate_markdown_table(events: List[AuthorisedCapitalChange], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    def esc(s: str) -> str:
        return (s or "").replace("\n", " ").replace("|", "\\|").strip()

    lines: List[str] = []
    lines.append("## Authorised Share Capital Change (Draft)\n")
    lines.append(
        "| meeting_date | meeting_type | old_capital_breakdown | new_capital_breakdown | source_documents | is_valid_sh7_event |"
    )
    lines.append(
        "|---|---|---|---|---|---|"
    )
    for e in events:
        lines.append(
            "| "
            + " | ".join(
                [
                    esc(e.meeting_date),
                    esc(e.meeting_type),
                    esc(e.old_capital_breakdown),
                    esc(e.new_capital_breakdown),
                    esc(", ".join(e.source_documents)),
                    "true" if e.is_valid_sh7_event else "false",
                ]
            )
            + " |"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def main() -> int:
    load_dotenv()
    setup_logging()

    logger = logging.getLogger("pipeline")
    logger.info("Starting pipeline")
    logger.info("OPENAI_API_KEY present=%s", bool(os.getenv("OPENAI_API_KEY")))

    data_directory = os.getenv("DATA_DIR", "data/sample_dataset/sh7")
    output_path = os.getenv("OUTPUT_PATH", "output/authorised_capital_changes.md")

    if not os.path.exists(data_directory):
        logger.error(
            "Could not find data directory: %s (run from repo root or set DATA_DIR).",
            data_directory,
        )
        return 2

    logger.info("Scanning %s for SH-7 packets...", data_directory)
    valid_events = process_all_events(data_directory)

    if valid_events:
        logger.info(
            "Extraction complete! Found %s valid SH-7 events. Writing output to %s",
            len(valid_events),
            output_path,
        )
        generate_markdown_table(valid_events, output_path=output_path)
    else:
        logger.warning(
            "No valid SH-7 events were extracted. Check your dataset or prompt logic."
        )
        generate_markdown_table([], output_path=output_path)
        logger.info("Wrote empty output table to %s", output_path)

    logger.info("Pipeline finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
