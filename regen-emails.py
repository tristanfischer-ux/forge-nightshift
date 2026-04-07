#!/usr/bin/env python3
"""
Regenerate all approved emails with better subject lines and shorter body copy.
Uses company data already in the DB — no Ollama needed.
"""

import sqlite3
import json
import random
import os

db_path = os.path.expanduser("~/Library/Application Support/com.fractionalforge.nightshift/nightshift.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# Get all approved emails with company data
rows = conn.execute("""
    SELECT e.id, e.company_id, e.to_email, e.claim_token,
           c.name, c.city, c.specialties, c.certifications, c.description,
           c.contact_name, c.category
    FROM emails e
    JOIN companies c ON c.id = e.company_id
    WHERE e.status = 'approved'
    ORDER BY e.created_at ASC
""").fetchall()

print(f"Regenerating {len(rows)} approved emails...\n")


def pick_specialty(specialties_json):
    """Pick one specific, short capability to mention."""
    try:
        specs = json.loads(specialties_json) if specialties_json else []
        if not isinstance(specs, list):
            specs = []
    except:
        specs = []
    # Filter out generic ones and long ones
    good = [s for s in specs if s and 5 < len(s) < 35 and "service" not in s.lower() and "support" not in s.lower() and "design and manufacture" not in s.lower()]
    return good[0] if good else None


def pick_cert(certs_json):
    """Pick a certification if notable."""
    try:
        certs = json.loads(certs_json) if certs_json else []
        if not isinstance(certs, list):
            certs = []
    except:
        certs = []
    notable = [c for c in certs if c and any(k in c.upper() for k in ["ISO", "AS9100", "NADCAP", "CE", "UKAS"])]
    return notable[0] if notable else None


def make_subject(name, specialty, cert, city):
    """Generate a short, varied subject line."""
    short_name = name.split(" Ltd")[0].split(" Limited")[0].split(" PLC")[0].strip()
    if len(short_name) > 25:
        short_name = short_name[:25].rsplit(" ", 1)[0]

    templates = []

    # Always available
    templates.append(f"Quick question about {short_name}")
    templates.append(f"{short_name} — got a minute?")

    if specialty:
        short_spec = specialty if len(specialty) < 30 else specialty[:28].rsplit(" ", 1)[0]
        templates.append(f"Your {short_spec} work")
        templates.append(f"{short_name} on Fractional Forge")

    if cert:
        templates.append(f"{short_name} — {cert} certified?")

    if city:
        templates.append(f"{short_spec if specialty else 'manufacturing'} in {city}?")

    return random.choice(templates)


def make_body(name, specialty, cert, city, description, contact_name, claim_token):
    """Generate a short, personal email body — 3-4 sentences max."""
    greeting = f"Hi {contact_name.split()[0]}," if contact_name and contact_name.strip() else "Hi,"

    # First sentence: what we did
    if specialty and city:
        opener = f"I came across {name} while looking into {specialty.lower()} in {city}."
    elif specialty:
        opener = f"I came across {name} while researching {specialty.lower()} in the UK."
    elif city:
        opener = f"I came across {name} while looking at manufacturers in {city}."
    else:
        opener = f"I came across {name} while researching UK manufacturers."

    # Second sentence: what we built
    middle = "I'm building a free directory of UK manufacturers — I've already put together a listing for you based on what I found."

    # CTA
    if claim_token:
        claim_url = f"https://fractionalforge.app/claim/{claim_token}"
        cta = f'Could you take a look and let me know if I got it right? <a href="{claim_url}">Check your listing</a>'
    else:
        cta = "Could you take a look and let me know if I got the details right?"

    # Sign-off
    signoff = 'Tristan<br/><a href="https://fractionalforge.app">fractionalforge.app</a>'

    return f"""<p style="margin:0 0 12px 0;">{greeting}</p>
<p style="margin:0 0 12px 0;">{opener}</p>
<p style="margin:0 0 12px 0;">{middle}</p>
<p style="margin:0 0 12px 0;">{cta}</p>
<p style="margin:0 0 12px 0;">{signoff}</p>"""


updated = 0
subjects_used = set()

for row in rows:
    specialty = pick_specialty(row["specialties"])
    cert = pick_cert(row["certifications"])
    city = row["city"] or ""
    name = row["name"]
    contact_name = row["contact_name"] or ""
    claim_token = row["claim_token"] or ""
    description = row["description"] or ""

    subject = make_subject(name, specialty, cert, city)

    # Ensure we don't repeat the exact same subject too many times
    base = subject
    attempt = 0
    while subject in subjects_used and attempt < 5:
        subject = make_subject(name, specialty, cert, city)
        attempt += 1
    subjects_used.add(subject)

    body = make_body(name, specialty, cert, city, description, contact_name, claim_token)

    conn.execute(
        "UPDATE emails SET subject=?, body=?, updated_at=datetime('now') WHERE id=?",
        (subject, body, row["id"])
    )
    updated += 1

    if updated <= 5:
        print(f"  [{updated}] {row['to_email']}")
        print(f"      Subject: {subject}")
        print()

conn.commit()
conn.close()

print(f"\nDone. {updated} emails regenerated.")
