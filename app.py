from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

from flask import Flask, flash, g, redirect, render_template, request, url_for


BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "app.db"

VIDEO_STAGES = [
    "Idea",
    "Approved",
    "Pre-production",
    "Filming",
    "Editing",
    "Review",
    "Scheduled",
    "Published",
]
DEAL_STATUSES = [
    "Negotiating",
    "Locked",
    "Delivered",
    "Awaiting payment",
    "Paid",
]
DEAL_TYPES = ["CPM", "Flat"]
EFFORT_OPTIONS = ["Low", "Medium", "High"]
TIMELINE_OPTIONS = ["Tight", "Feasible", "Flexible"]
CONCEPT_TYPES = [
    "Only ate X",
    "Doubled budget",
    "Challenge",
    "Behind the scenes",
    "Transformation",
    "Travel",
    "Collab",
    "Review",
    "Experiment",
    "Other",
]
TEAM_ROLES = ["Creator", "Editor", "Thumbnail", "Producer", "Reviewer"]
PRODUCTION_STAGES = {"Approved", "Pre-production", "Filming", "Editing", "Review"}

STAGE_ALIASES = {
    "pre-production": "Pre-production",
    "filming": "Filming",
    "editing": "Editing",
    "published": "Published",
}
DEAL_STATUS_ALIASES = {
    "locked": "Locked",
    "fulfilled": "Delivered",
    "paid": "Paid",
}


app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key"


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(_error: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def column_names(table_name: str) -> set[str]:
    return {
        row["name"] for row in get_db().execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def ensure_column(table_name: str, definition: str) -> None:
    column_name = definition.split()[0]
    if column_name not in column_names(table_name):
        get_db().execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            stage TEXT NOT NULL DEFAULT 'Idea',
            notes TEXT NOT NULL DEFAULT '',
            checklist TEXT NOT NULL DEFAULT '',
            concept_type TEXT,
            tags TEXT NOT NULL DEFAULT '',
            effort TEXT,
            estimated_views INTEGER,
            expected_views INTEGER,
            actual_views INTEGER,
            length_minutes INTEGER,
            sponsor_potential INTEGER NOT NULL DEFAULT 0,
            timeline_feasibility TEXT,
            shoot_date TEXT,
            publish_deadline TEXT,
            sponsor_deadline TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name TEXT NOT NULL,
            deal_value REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'Negotiating',
            deal_type TEXT NOT NULL DEFAULT 'Flat',
            view_guarantee INTEGER,
            video_id INTEGER UNIQUE,
            deadline TEXT,
            notes TEXT NOT NULL DEFAULT '',
            fulfillment_month TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            tags TEXT NOT NULL DEFAULT '',
            concept_type TEXT,
            effort TEXT,
            estimated_views INTEGER,
            sponsor_potential INTEGER NOT NULL DEFAULT 0,
            timeline_feasibility TEXT,
            notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'Open',
            promoted_video_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (promoted_video_id) REFERENCES videos(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS team_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            specialty TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS video_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id INTEGER NOT NULL,
            team_member_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            UNIQUE (video_id, role),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (team_member_id) REFERENCES team_members(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_deals_video_id_unique
        ON deals(video_id) WHERE video_id IS NOT NULL;
        """
    )

    for definition in [
        "description TEXT NOT NULL DEFAULT ''",
        "checklist TEXT NOT NULL DEFAULT ''",
        "concept_type TEXT",
        "tags TEXT NOT NULL DEFAULT ''",
        "effort TEXT",
        "estimated_views INTEGER",
        "expected_views INTEGER",
        "actual_views INTEGER",
        "length_minutes INTEGER",
        "sponsor_potential INTEGER NOT NULL DEFAULT 0",
        "timeline_feasibility TEXT",
        "shoot_date TEXT",
        "publish_deadline TEXT",
        "sponsor_deadline TEXT",
    ]:
        ensure_column("videos", definition)

    for definition in [
        "deal_type TEXT NOT NULL DEFAULT 'Flat'",
        "view_guarantee INTEGER",
        "deadline TEXT",
        "notes TEXT NOT NULL DEFAULT ''",
        "video_id INTEGER",
        "fulfillment_month TEXT",
    ]:
        ensure_column("deals", definition)

    for definition in [
        "concept_type TEXT",
        "effort TEXT",
        "estimated_views INTEGER",
        "sponsor_potential INTEGER NOT NULL DEFAULT 0",
        "timeline_feasibility TEXT",
        "notes TEXT NOT NULL DEFAULT ''",
        "status TEXT NOT NULL DEFAULT 'Open'",
        "promoted_video_id INTEGER",
    ]:
        ensure_column("ideas", definition)

    normalize_existing_data(db)
    seed_team_members_if_empty(db)
    db.commit()


def normalize_existing_data(db: sqlite3.Connection) -> None:
    for old_stage, new_stage in STAGE_ALIASES.items():
        db.execute("UPDATE videos SET stage = ? WHERE LOWER(stage) = ?", (new_stage, old_stage))

    for old_status, new_status in DEAL_STATUS_ALIASES.items():
        db.execute("UPDATE deals SET status = ? WHERE LOWER(status) = ?", (new_status, old_status))

    db.execute("UPDATE deals SET deal_type = 'Flat' WHERE deal_type IS NULL OR deal_type = ''")
    db.execute("UPDATE deals SET notes = '' WHERE notes IS NULL")
    db.execute("UPDATE videos SET description = '' WHERE description IS NULL")
    db.execute("UPDATE videos SET checklist = '' WHERE checklist IS NULL")
    db.execute("UPDATE videos SET tags = '' WHERE tags IS NULL")
    db.execute("UPDATE videos SET sponsor_potential = 0 WHERE sponsor_potential IS NULL")


def seed_team_members_if_empty(db: sqlite3.Connection) -> None:
    member_count = db.execute("SELECT COUNT(*) AS count FROM team_members").fetchone()["count"]
    if member_count:
        return
    db.executemany(
        "INSERT INTO team_members (name, specialty) VALUES (?, ?)",
        [
            ("Alex", "Creator"),
            ("Sam", "Editor"),
            ("Jordan", "Thumbnail"),
        ],
    )


def fetch_video(video_id: int) -> sqlite3.Row:
    video = get_db().execute("SELECT * FROM videos WHERE id = ?", (video_id,)).fetchone()
    if video is None:
        raise ValueError("Video not found")
    return video


def fetch_deal(deal_id: int) -> sqlite3.Row:
    deal = get_db().execute("SELECT * FROM deals WHERE id = ?", (deal_id,)).fetchone()
    if deal is None:
        raise ValueError("Deal not found")
    return deal


def fetch_idea(idea_id: int) -> sqlite3.Row:
    idea = get_db().execute("SELECT * FROM ideas WHERE id = ?", (idea_id,)).fetchone()
    if idea is None:
        raise ValueError("Idea not found")
    return idea


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def coerce_int(raw_value: str, field_label: str, allow_blank: bool = True) -> tuple[int | None, str | None]:
    value = raw_value.strip()
    if not value:
        return (None, None) if allow_blank else (None, f"{field_label} is required.")
    try:
        parsed = int(value)
    except ValueError:
        return None, f"{field_label} must be a whole number."
    if parsed < 0:
        return None, f"{field_label} cannot be negative."
    return parsed, None


def coerce_float(raw_value: str, field_label: str) -> tuple[float | None, str | None]:
    value = raw_value.strip()
    if not value:
        return None, f"{field_label} is required."
    try:
        parsed = float(value)
    except ValueError:
        return None, f"{field_label} must be a number."
    if parsed < 0:
        return None, f"{field_label} cannot be negative."
    return parsed, None


def month_window(anchor: date) -> tuple[str, str]:
    start = anchor.replace(day=1)
    next_month = date(anchor.year + (anchor.month // 12), (anchor.month % 12) + 1, 1)
    return start.isoformat(), next_month.isoformat()


def stage_rank(stage: str | None) -> int:
    try:
        return VIDEO_STAGES.index(stage or "Idea")
    except ValueError:
        return 0


def deal_status_rank(status: str | None) -> int:
    try:
        return DEAL_STATUSES.index(status or "Negotiating")
    except ValueError:
        return 0


def risk_for_video(video: sqlite3.Row, today: date) -> dict[str, object]:
    publish_deadline = parse_date(video["publish_deadline"])
    sponsor_deadline = parse_date(video["sponsor_deadline"])
    deal_deadline = parse_date(video["deal_deadline"])
    active_deadline = sponsor_deadline or deal_deadline or publish_deadline
    stage = video["stage"]
    linked_deal = video["deal_id"] is not None
    overdue = bool(active_deadline and active_deadline < today and stage != "Published")
    deadline_in_days = (active_deadline - today).days if active_deadline else None
    upcoming = bool(
        active_deadline and stage != "Published" and 0 <= deadline_in_days <= 14
    )

    view_gap = None
    if video["view_guarantee"] and video["expected_views"] is not None:
        view_gap = video["expected_views"] - video["view_guarantee"]

    views_under_goal = bool(
        video["view_guarantee"]
        and (
            (stage == "Published" and video["actual_views"] is not None and video["actual_views"] < video["view_guarantee"])
            or (
                stage != "Published"
                and video["expected_views"] is not None
                and video["expected_views"] < video["view_guarantee"]
            )
        )
    )
    pipeline_late = linked_deal and stage_rank(stage) < stage_rank("Review") and deadline_in_days is not None and deadline_in_days <= 7
    sponsor_risk = linked_deal and (pipeline_late or views_under_goal)

    if overdue or sponsor_risk:
        risk = "red"
        label = "At risk"
    elif upcoming:
        risk = "yellow"
        label = "Upcoming"
    else:
        risk = "green"
        label = "On track"

    return {
        "risk": risk,
        "label": label,
        "overdue": overdue,
        "upcoming": upcoming,
        "deadline_in_days": deadline_in_days,
        "active_deadline": active_deadline.isoformat() if active_deadline else None,
        "sponsor_risk": sponsor_risk,
        "views_under_goal": views_under_goal,
        "view_gap": view_gap,
    }


def ranked_idea_score(idea: sqlite3.Row) -> float:
    effort_weight = {"Low": 14, "Medium": 8, "High": 2}.get(idea["effort"], 6)
    timeline_weight = {"Flexible": 12, "Feasible": 8, "Tight": 2}.get(
        idea["timeline_feasibility"], 6
    )
    sponsor_weight = 16 if idea["sponsor_potential"] else 0
    estimated_views = idea["estimated_views"] or 0
    return effort_weight + timeline_weight + sponsor_weight + (estimated_views / 10000)


def load_team_members() -> list[sqlite3.Row]:
    return get_db().execute(
        "SELECT * FROM team_members ORDER BY name COLLATE NOCASE ASC"
    ).fetchall()


def load_assignments_map(video_id: int) -> dict[str, int]:
    rows = get_db().execute(
        """
        SELECT role, team_member_id
        FROM video_assignments
        WHERE video_id = ?
        """,
        (video_id,),
    ).fetchall()
    return {row["role"]: row["team_member_id"] for row in rows}


def save_assignments(video_id: int, role_to_member: dict[str, int | None]) -> None:
    db = get_db()
    db.execute("DELETE FROM video_assignments WHERE video_id = ?", (video_id,))
    for role, member_id in role_to_member.items():
        if member_id:
            db.execute(
                "INSERT INTO video_assignments (video_id, team_member_id, role) VALUES (?, ?, ?)",
                (video_id, member_id, role),
            )


def available_deals_for_video(video_id: int | None = None) -> list[sqlite3.Row]:
    if video_id is None:
        return get_db().execute(
            """
            SELECT id, brand_name, status, deal_value
            FROM deals
            WHERE video_id IS NULL
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
    return get_db().execute(
        """
        SELECT id, brand_name, status, deal_value
        FROM deals
        WHERE video_id IS NULL OR video_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (video_id,),
    ).fetchall()


def current_deal_for_video(video_id: int) -> sqlite3.Row | None:
    return get_db().execute(
        """
        SELECT *
        FROM deals
        WHERE video_id = ?
        LIMIT 1
        """,
        (video_id,),
    ).fetchone()


def sync_video_deal(video_id: int, deal_id: int | None) -> None:
    db = get_db()
    db.execute("UPDATE deals SET video_id = NULL WHERE video_id = ?", (video_id,))
    if deal_id is not None:
        db.execute("UPDATE deals SET video_id = ? WHERE id = ?", (video_id, deal_id))


def load_videos() -> list[dict[str, object]]:
    rows = get_db().execute(
        """
        SELECT videos.*,
               deals.id AS deal_id,
               deals.brand_name,
               deals.deal_value,
               deals.status AS deal_status,
               deals.deal_type,
               deals.view_guarantee,
               deals.deadline AS deal_deadline
        FROM videos
        LEFT JOIN deals ON deals.video_id = videos.id
        ORDER BY
            CASE videos.stage
                WHEN 'Idea' THEN 1
                WHEN 'Approved' THEN 2
                WHEN 'Pre-production' THEN 3
                WHEN 'Filming' THEN 4
                WHEN 'Editing' THEN 5
                WHEN 'Review' THEN 6
                WHEN 'Scheduled' THEN 7
                WHEN 'Published' THEN 8
                ELSE 9
            END,
            COALESCE(videos.publish_deadline, videos.sponsor_deadline, videos.created_at) ASC
        """
    ).fetchall()
    today = date.today()
    enriched = []
    for row in rows:
        risk = risk_for_video(row, today)
        item = dict(row)
        item.update(risk)
        enriched.append(item)
    return enriched


def video_detail_data(video_id: int) -> dict[str, object]:
    video = get_db().execute(
        """
        SELECT videos.*,
               deals.id AS deal_id,
               deals.brand_name,
               deals.deal_value,
               deals.status AS deal_status,
               deals.deal_type,
               deals.view_guarantee,
               deals.deadline AS deal_deadline,
               deals.notes AS deal_notes
        FROM videos
        LEFT JOIN deals ON deals.video_id = videos.id
        WHERE videos.id = ?
        """,
        (video_id,),
    ).fetchone()
    if video is None:
        raise ValueError("Video not found")

    assignments = get_db().execute(
        """
        SELECT video_assignments.role, team_members.name, team_members.id AS member_id
        FROM video_assignments
        JOIN team_members ON team_members.id = video_assignments.team_member_id
        WHERE video_assignments.video_id = ?
        ORDER BY team_members.name COLLATE NOCASE ASC
        """,
        (video_id,),
    ).fetchall()
    risk = risk_for_video(video, date.today())
    item = dict(video)
    item.update(risk)
    item["assignments"] = assignments
    return item


def validate_video_form(video_id: int | None = None) -> tuple[dict[str, object] | None, list[str], dict[str, str]]:
    form = request.form
    errors: list[str] = []

    title = form.get("title", "").strip()
    description = form.get("description", "").strip()
    stage = form.get("stage", "").strip()
    notes = form.get("notes", "").strip()
    checklist = form.get("checklist", "").strip()
    concept_type = form.get("concept_type", "").strip() or None
    tags = form.get("tags", "").strip()
    effort = form.get("effort", "").strip() or None
    timeline_feasibility = form.get("timeline_feasibility", "").strip() or None
    sponsor_potential = 1 if form.get("sponsor_potential") == "on" else 0
    shoot_date = form.get("shoot_date", "").strip() or None
    publish_deadline = form.get("publish_deadline", "").strip() or None
    sponsor_deadline = form.get("sponsor_deadline", "").strip() or None
    linked_deal_raw = form.get("linked_deal_id", "").strip()

    estimated_views, error = coerce_int(form.get("estimated_views", ""), "Estimated views")
    if error:
        errors.append(error)
    expected_views, error = coerce_int(form.get("expected_views", ""), "Expected views")
    if error:
        errors.append(error)
    actual_views, error = coerce_int(form.get("actual_views", ""), "Actual views")
    if error:
        errors.append(error)
    length_minutes, error = coerce_int(form.get("length_minutes", ""), "Length")
    if error:
        errors.append(error)

    if not title:
        errors.append("Title is required.")
    if stage not in VIDEO_STAGES:
        errors.append("Please choose a valid stage.")
    if effort and effort not in EFFORT_OPTIONS:
        errors.append("Please choose a valid effort level.")
    if timeline_feasibility and timeline_feasibility not in TIMELINE_OPTIONS:
        errors.append("Please choose a valid timeline feasibility.")
    if concept_type and concept_type not in CONCEPT_TYPES:
        errors.append("Please choose a valid concept type.")

    linked_deal_id: int | None = None
    if linked_deal_raw:
        try:
            linked_deal_id = int(linked_deal_raw)
        except ValueError:
            errors.append("Please choose a valid linked deal.")
        else:
            existing = get_db().execute(
                "SELECT id, video_id FROM deals WHERE id = ?", (linked_deal_id,)
            ).fetchone()
            if existing is None:
                errors.append("Please choose a valid linked deal.")
            elif existing["video_id"] not in (None, video_id):
                errors.append("That deal is already attached to another video.")

    role_assignments: dict[str, int | None] = {}
    member_ids = {str(member["id"]) for member in load_team_members()}
    for role in TEAM_ROLES:
        key = f"role_{role.lower()}"
        raw_value = form.get(key, "").strip()
        if raw_value:
            if raw_value not in member_ids:
                errors.append(f"Please choose a valid team member for {role.lower()}.")
                role_assignments[role] = None
            else:
                role_assignments[role] = int(raw_value)
        else:
            role_assignments[role] = None

    payload = {
        "title": title,
        "description": description,
        "stage": stage,
        "notes": notes,
        "checklist": checklist,
        "concept_type": concept_type,
        "tags": tags,
        "effort": effort,
        "estimated_views": estimated_views,
        "expected_views": expected_views,
        "actual_views": actual_views,
        "length_minutes": length_minutes,
        "sponsor_potential": sponsor_potential,
        "timeline_feasibility": timeline_feasibility,
        "shoot_date": shoot_date,
        "publish_deadline": publish_deadline,
        "sponsor_deadline": sponsor_deadline,
        "linked_deal_id": linked_deal_id,
    }
    return (payload if not errors else None), errors, {role: str(member_id or "") for role, member_id in role_assignments.items()}


def validate_deal_form(deal_id: int | None = None) -> tuple[dict[str, object] | None, list[str]]:
    form = request.form
    errors: list[str] = []

    brand_name = form.get("brand_name", "").strip()
    status = form.get("status", "").strip()
    deal_type = form.get("deal_type", "").strip()
    deadline = form.get("deadline", "").strip() or None
    fulfillment_month = form.get("fulfillment_month", "").strip() or None
    notes = form.get("notes", "").strip()
    linked_video_raw = form.get("video_id", "").strip()

    deal_value, error = coerce_float(form.get("deal_value", ""), "Deal value")
    if error:
        errors.append(error)
    view_guarantee, error = coerce_int(form.get("view_guarantee", ""), "View guarantee")
    if error:
        errors.append(error)

    if not brand_name:
        errors.append("Brand name is required.")
    if status not in DEAL_STATUSES:
        errors.append("Please choose a valid deal status.")
    if deal_type not in DEAL_TYPES:
        errors.append("Please choose a valid deal type.")

    video_id: int | None = None
    if linked_video_raw:
        try:
            video_id = int(linked_video_raw)
        except ValueError:
            errors.append("Please choose a valid linked video.")
        else:
            video = get_db().execute("SELECT id FROM videos WHERE id = ?", (video_id,)).fetchone()
            if video is None:
                errors.append("Please choose a valid linked video.")
            else:
                occupying = get_db().execute(
                    "SELECT id FROM deals WHERE video_id = ? AND id != ?",
                    (video_id, deal_id or 0),
                ).fetchone()
                if occupying is not None:
                    errors.append("That video already has a linked deal.")

    payload = {
        "brand_name": brand_name,
        "deal_value": deal_value,
        "status": status,
        "deal_type": deal_type,
        "view_guarantee": view_guarantee,
        "video_id": video_id,
        "deadline": deadline,
        "fulfillment_month": fulfillment_month,
        "notes": notes,
    }
    return (payload if not errors else None), errors


@app.route("/")
def dashboard():
    db = get_db()
    today = date.today()
    month_start, next_month = month_window(today)

    metrics = db.execute(
        """
        SELECT
            COALESCE(SUM(CASE
                WHEN status != 'Negotiating' AND deadline >= ? AND deadline < ? THEN deal_value
                ELSE 0
            END), 0) AS revenue_this_month,
            COALESCE(SUM(CASE
                WHEN status != 'Negotiating' THEN view_guarantee
                ELSE 0
            END), 0) AS total_required_views
        FROM deals
        """,
        (month_start, next_month),
    ).fetchone()

    all_videos = load_videos()
    videos_in_production = sum(1 for video in all_videos if video["stage"] in PRODUCTION_STAGES)
    videos_at_risk = [video for video in all_videos if video["risk"] == "red"]
    upcoming_deadlines = [video for video in all_videos if video["risk"] == "yellow"]
    on_track = [video for video in all_videos if video["risk"] == "green" and video["stage"] != "Published"]

    recent_performance = db.execute(
        """
        SELECT title, concept_type, actual_views, publish_deadline, stage
        FROM videos
        WHERE actual_views IS NOT NULL
        ORDER BY COALESCE(publish_deadline, created_at) DESC
        LIMIT 5
        """
    ).fetchall()

    next_best_videos = db.execute(
        """
        SELECT *
        FROM ideas
        WHERE status != 'Promoted'
        ORDER BY created_at DESC
        LIMIT 20
        """
    ).fetchall()
    ranked_ideas = sorted(
        [dict(idea) | {"score": ranked_idea_score(idea)} for idea in next_best_videos],
        key=lambda idea: idea["score"],
        reverse=True,
    )[:5]

    return render_template(
        "dashboard.html",
        metrics=metrics,
        videos_in_production=videos_in_production,
        videos_at_risk=videos_at_risk[:5],
        videos_at_risk_count=len(videos_at_risk),
        upcoming_deadlines=upcoming_deadlines[:5],
        on_track=on_track[:5],
        recent_performance=recent_performance,
        ranked_ideas=ranked_ideas,
        all_videos=all_videos[:6],
    )


@app.route("/videos")
def videos_index():
    return render_template("videos.html", videos=load_videos())


@app.route("/videos/new", methods=["GET", "POST"])
def create_video():
    team_members = load_team_members()
    deal_options = available_deals_for_video()
    selected_assignments = {role: "" for role in TEAM_ROLES}

    if request.method == "POST":
        payload, errors, selected_assignments = validate_video_form()
        if errors:
            for error in errors:
                flash(error)
        else:
            db = get_db()
            cursor = db.execute(
                """
                INSERT INTO videos (
                    title, description, stage, notes, checklist, concept_type, tags,
                    effort, estimated_views, expected_views, actual_views, length_minutes,
                    sponsor_potential, timeline_feasibility, shoot_date, publish_deadline,
                    sponsor_deadline
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["title"],
                    payload["description"],
                    payload["stage"],
                    payload["notes"],
                    payload["checklist"],
                    payload["concept_type"],
                    payload["tags"],
                    payload["effort"],
                    payload["estimated_views"],
                    payload["expected_views"],
                    payload["actual_views"],
                    payload["length_minutes"],
                    payload["sponsor_potential"],
                    payload["timeline_feasibility"],
                    payload["shoot_date"],
                    payload["publish_deadline"],
                    payload["sponsor_deadline"],
                ),
            )
            video_id = cursor.lastrowid
            save_assignments(
                video_id,
                {role: int(value) if value else None for role, value in selected_assignments.items()},
            )
            sync_video_deal(video_id, payload["linked_deal_id"])
            db.commit()
            flash("Video created.")
            return redirect(url_for("video_detail", video_id=video_id))

    return render_template(
        "video_form.html",
        video=None,
        stage_options=VIDEO_STAGES,
        concept_types=CONCEPT_TYPES,
        effort_options=EFFORT_OPTIONS,
        timeline_options=TIMELINE_OPTIONS,
        team_roles=TEAM_ROLES,
        team_members=team_members,
        selected_assignments=selected_assignments,
        deal_options=deal_options,
        form_title="Add Video",
        submit_label="Create Video",
    )


@app.route("/videos/<int:video_id>")
def video_detail(video_id: int):
    try:
        video = video_detail_data(video_id)
    except ValueError:
        flash("Video not found.")
        return redirect(url_for("videos_index"))
    return render_template("video_detail.html", video=video)


@app.route("/videos/<int:video_id>/edit", methods=["GET", "POST"])
def edit_video(video_id: int):
    try:
        video = fetch_video(video_id)
    except ValueError:
        flash("Video not found.")
        return redirect(url_for("videos_index"))

    team_members = load_team_members()
    deal_options = available_deals_for_video(video_id)
    existing_deal = current_deal_for_video(video_id)
    selected_assignments = {
        role: str(member_id) for role, member_id in load_assignments_map(video_id).items()
    }
    for role in TEAM_ROLES:
        selected_assignments.setdefault(role, "")

    if request.method == "POST":
        payload, errors, selected_assignments = validate_video_form(video_id)
        if errors:
            for error in errors:
                flash(error)
        else:
            db = get_db()
            db.execute(
                """
                UPDATE videos
                SET title = ?, description = ?, stage = ?, notes = ?, checklist = ?,
                    concept_type = ?, tags = ?, effort = ?, estimated_views = ?,
                    expected_views = ?, actual_views = ?, length_minutes = ?,
                    sponsor_potential = ?, timeline_feasibility = ?, shoot_date = ?,
                    publish_deadline = ?, sponsor_deadline = ?
                WHERE id = ?
                """,
                (
                    payload["title"],
                    payload["description"],
                    payload["stage"],
                    payload["notes"],
                    payload["checklist"],
                    payload["concept_type"],
                    payload["tags"],
                    payload["effort"],
                    payload["estimated_views"],
                    payload["expected_views"],
                    payload["actual_views"],
                    payload["length_minutes"],
                    payload["sponsor_potential"],
                    payload["timeline_feasibility"],
                    payload["shoot_date"],
                    payload["publish_deadline"],
                    payload["sponsor_deadline"],
                    video_id,
                ),
            )
            save_assignments(
                video_id,
                {role: int(value) if value else None for role, value in selected_assignments.items()},
            )
            sync_video_deal(video_id, payload["linked_deal_id"])
            db.commit()
            flash("Video updated.")
            return redirect(url_for("video_detail", video_id=video_id))

    return render_template(
        "video_form.html",
        video=video,
        current_deal=existing_deal,
        stage_options=VIDEO_STAGES,
        concept_types=CONCEPT_TYPES,
        effort_options=EFFORT_OPTIONS,
        timeline_options=TIMELINE_OPTIONS,
        team_roles=TEAM_ROLES,
        team_members=team_members,
        selected_assignments=selected_assignments,
        deal_options=deal_options,
        form_title="Edit Video",
        submit_label="Save Changes",
    )


@app.post("/videos/<int:video_id>/delete")
def delete_video(video_id: int):
    db = get_db()
    db.execute("UPDATE deals SET video_id = NULL WHERE video_id = ?", (video_id,))
    db.execute("UPDATE ideas SET promoted_video_id = NULL WHERE promoted_video_id = ?", (video_id,))
    db.execute("DELETE FROM videos WHERE id = ?", (video_id,))
    db.commit()
    flash("Video deleted.")
    return redirect(url_for("videos_index"))


@app.route("/deals")
def deals_index():
    deals = get_db().execute(
        """
        SELECT deals.*, videos.title AS video_title, videos.stage AS video_stage
        FROM deals
        LEFT JOIN videos ON videos.id = deals.video_id
        ORDER BY
            CASE deals.status
                WHEN 'Negotiating' THEN 1
                WHEN 'Locked' THEN 2
                WHEN 'Delivered' THEN 3
                WHEN 'Awaiting payment' THEN 4
                WHEN 'Paid' THEN 5
                ELSE 6
            END,
            COALESCE(deals.deadline, deals.created_at) ASC
        """
    ).fetchall()
    return render_template("deals.html", deals=deals)


@app.route("/deals/new", methods=["GET", "POST"])
def create_deal():
    videos = get_db().execute(
        """
        SELECT id, title, stage
        FROM videos
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()

    if request.method == "POST":
        payload, errors = validate_deal_form()
        if errors:
            for error in errors:
                flash(error)
        else:
            get_db().execute(
                """
                INSERT INTO deals (
                    brand_name, deal_value, status, deal_type, view_guarantee, video_id,
                    deadline, fulfillment_month, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["brand_name"],
                    payload["deal_value"],
                    payload["status"],
                    payload["deal_type"],
                    payload["view_guarantee"],
                    payload["video_id"],
                    payload["deadline"],
                    payload["fulfillment_month"],
                    payload["notes"],
                ),
            )
            get_db().commit()
            flash("Deal created.")
            return redirect(url_for("deals_index"))

    return render_template(
        "deal_form.html",
        deal=None,
        videos=videos,
        status_options=DEAL_STATUSES,
        deal_types=DEAL_TYPES,
        form_title="Add Deal",
        submit_label="Create Deal",
    )


@app.route("/deals/<int:deal_id>/edit", methods=["GET", "POST"])
def edit_deal(deal_id: int):
    try:
        deal = fetch_deal(deal_id)
    except ValueError:
        flash("Deal not found.")
        return redirect(url_for("deals_index"))

    videos = get_db().execute(
        """
        SELECT id, title, stage
        FROM videos
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()

    if request.method == "POST":
        payload, errors = validate_deal_form(deal_id)
        if errors:
            for error in errors:
                flash(error)
        else:
            get_db().execute(
                """
                UPDATE deals
                SET brand_name = ?, deal_value = ?, status = ?, deal_type = ?,
                    view_guarantee = ?, video_id = ?, deadline = ?, fulfillment_month = ?,
                    notes = ?
                WHERE id = ?
                """,
                (
                    payload["brand_name"],
                    payload["deal_value"],
                    payload["status"],
                    payload["deal_type"],
                    payload["view_guarantee"],
                    payload["video_id"],
                    payload["deadline"],
                    payload["fulfillment_month"],
                    payload["notes"],
                    deal_id,
                ),
            )
            get_db().commit()
            flash("Deal updated.")
            return redirect(url_for("deals_index"))

    return render_template(
        "deal_form.html",
        deal=deal,
        videos=videos,
        status_options=DEAL_STATUSES,
        deal_types=DEAL_TYPES,
        form_title="Edit Deal",
        submit_label="Save Changes",
    )


@app.post("/deals/<int:deal_id>/delete")
def delete_deal(deal_id: int):
    get_db().execute("DELETE FROM deals WHERE id = ?", (deal_id,))
    get_db().commit()
    flash("Deal deleted.")
    return redirect(url_for("deals_index"))


@app.route("/ideas")
def ideas_index():
    rows = get_db().execute(
        """
        SELECT ideas.*, videos.title AS promoted_video_title
        FROM ideas
        LEFT JOIN videos ON videos.id = ideas.promoted_video_id
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()
    ideas = [dict(row) | {"score": ranked_idea_score(row)} for row in rows]
    ideas.sort(key=lambda row: row["score"], reverse=True)
    return render_template("ideas.html", ideas=ideas)


@app.post("/ideas/quick-add")
def quick_add_idea():
    title = request.form.get("title", "").strip()
    tags = request.form.get("tags", "").strip()
    if not title:
        flash("Idea title is required.")
        return redirect(url_for("dashboard"))
    get_db().execute("INSERT INTO ideas (title, tags) VALUES (?, ?)", (title, tags))
    get_db().commit()
    flash("Idea captured.")
    return redirect(url_for("dashboard"))


@app.route("/ideas/new", methods=["GET", "POST"])
def create_idea():
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        tags = request.form.get("tags", "").strip()
        concept_type = request.form.get("concept_type", "").strip() or None
        effort = request.form.get("effort", "").strip() or None
        timeline_feasibility = request.form.get("timeline_feasibility", "").strip() or None
        notes = request.form.get("notes", "").strip()
        sponsor_potential = 1 if request.form.get("sponsor_potential") == "on" else 0
        estimated_views, error = coerce_int(request.form.get("estimated_views", ""), "Estimated views")
        if not title:
            flash("Idea title is required.")
        elif error:
            flash(error)
        else:
            get_db().execute(
                """
                INSERT INTO ideas (
                    title, tags, concept_type, effort, estimated_views,
                    sponsor_potential, timeline_feasibility, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    tags,
                    concept_type,
                    effort,
                    estimated_views,
                    sponsor_potential,
                    timeline_feasibility,
                    notes,
                ),
            )
            get_db().commit()
            flash("Idea created.")
            return redirect(url_for("ideas_index"))

    return render_template(
        "idea_form.html",
        idea=None,
        concept_types=CONCEPT_TYPES,
        effort_options=EFFORT_OPTIONS,
        timeline_options=TIMELINE_OPTIONS,
        form_title="Add Idea",
        submit_label="Create Idea",
    )


@app.route("/ideas/<int:idea_id>/edit", methods=["GET", "POST"])
def edit_idea(idea_id: int):
    try:
        idea = fetch_idea(idea_id)
    except ValueError:
        flash("Idea not found.")
        return redirect(url_for("ideas_index"))

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        tags = request.form.get("tags", "").strip()
        concept_type = request.form.get("concept_type", "").strip() or None
        effort = request.form.get("effort", "").strip() or None
        timeline_feasibility = request.form.get("timeline_feasibility", "").strip() or None
        notes = request.form.get("notes", "").strip()
        sponsor_potential = 1 if request.form.get("sponsor_potential") == "on" else 0
        estimated_views, error = coerce_int(request.form.get("estimated_views", ""), "Estimated views")
        status = request.form.get("status", "").strip() or "Open"
        if not title:
            flash("Idea title is required.")
        elif error:
            flash(error)
        else:
            get_db().execute(
                """
                UPDATE ideas
                SET title = ?, tags = ?, concept_type = ?, effort = ?, estimated_views = ?,
                    sponsor_potential = ?, timeline_feasibility = ?, notes = ?, status = ?
                WHERE id = ?
                """,
                (
                    title,
                    tags,
                    concept_type,
                    effort,
                    estimated_views,
                    sponsor_potential,
                    timeline_feasibility,
                    notes,
                    status,
                    idea_id,
                ),
            )
            get_db().commit()
            flash("Idea updated.")
            return redirect(url_for("ideas_index"))

    return render_template(
        "idea_form.html",
        idea=idea,
        concept_types=CONCEPT_TYPES,
        effort_options=EFFORT_OPTIONS,
        timeline_options=TIMELINE_OPTIONS,
        form_title="Edit Idea",
        submit_label="Save Changes",
    )


@app.post("/ideas/<int:idea_id>/promote")
def promote_idea(idea_id: int):
    try:
        idea = fetch_idea(idea_id)
    except ValueError:
        flash("Idea not found.")
        return redirect(url_for("ideas_index"))

    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO videos (
            title, description, stage, concept_type, tags, effort, estimated_views,
            sponsor_potential, timeline_feasibility, notes
        ) VALUES (?, ?, 'Idea', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            idea["title"],
            idea["notes"] or "",
            idea["concept_type"],
            idea["tags"],
            idea["effort"],
            idea["estimated_views"],
            idea["sponsor_potential"],
            idea["timeline_feasibility"],
            idea["notes"],
        ),
    )
    video_id = cursor.lastrowid
    db.execute(
        "UPDATE ideas SET promoted_video_id = ?, status = 'Promoted' WHERE id = ?",
        (video_id, idea_id),
    )
    db.commit()
    flash("Idea promoted into the video pipeline.")
    return redirect(url_for("edit_video", video_id=video_id))


@app.route("/analytics")
def analytics():
    concept_rows = get_db().execute(
        """
        SELECT concept_type,
               COUNT(*) AS video_count,
               ROUND(AVG(actual_views), 0) AS average_views,
               MAX(actual_views) AS top_views
        FROM videos
        WHERE concept_type IS NOT NULL AND concept_type != '' AND actual_views IS NOT NULL
        GROUP BY concept_type
        ORDER BY average_views DESC
        """
    ).fetchall()

    recent_performance = get_db().execute(
        """
        SELECT title, concept_type, actual_views, stage
        FROM videos
        WHERE actual_views IS NOT NULL
        ORDER BY actual_views DESC
        LIMIT 10
        """
    ).fetchall()
    return render_template(
        "analytics.html",
        concept_rows=concept_rows,
        recent_performance=recent_performance,
    )


@app.route("/team", methods=["GET", "POST"])
def team():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        specialty = request.form.get("specialty", "").strip()
        if not name:
            flash("Team member name is required.")
        else:
            get_db().execute(
                "INSERT INTO team_members (name, specialty) VALUES (?, ?)",
                (name, specialty),
            )
            get_db().commit()
            flash("Team member added.")
            return redirect(url_for("team"))

    team_members = load_team_members()
    assignments = get_db().execute(
        """
        SELECT team_members.id, COUNT(video_assignments.id) AS assignment_count
        FROM team_members
        LEFT JOIN video_assignments ON video_assignments.team_member_id = team_members.id
        GROUP BY team_members.id
        """
    ).fetchall()
    counts = {row["id"]: row["assignment_count"] for row in assignments}
    return render_template("team.html", team_members=team_members, counts=counts)


@app.post("/team/<int:member_id>/delete")
def delete_team_member(member_id: int):
    db = get_db()
    db.execute("DELETE FROM team_members WHERE id = ?", (member_id,))
    db.commit()
    flash("Team member removed.")
    return redirect(url_for("team"))


@app.context_processor
def inject_helpers():
    return {
        "video_stages": VIDEO_STAGES,
        "deal_status_options": DEAL_STATUSES,
        "team_roles": TEAM_ROLES,
    }


with app.app_context():
    init_db()


if __name__ == "__main__":
    app.run(debug=True)
