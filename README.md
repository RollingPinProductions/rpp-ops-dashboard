# YouTube Ops Manager

A lightweight operating system for a YouTube creator business.

## What it does

- Run a structured video pipeline from `Idea` to `Published`
- Track sponsor deals, delivery deadlines, payments, and view guarantees
- Capture ideas fast, then rank them by effort, expected upside, sponsor potential, and timeline
- Flag at-risk videos, upcoming deadlines, and sponsor fulfillment problems
- Manage a simple production team for creator, editor, thumbnail, producer, and reviewer roles
- View analytics by concept type and average views
- Store everything in SQLite with no separate database server

## Step-by-step setup

1. Open this folder in your terminal.
2. Create a virtual environment:

   ```bash
   python3 -m venv .venv
   ```

3. Activate it:

   ```bash
   source .venv/bin/activate
   ```

4. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

5. Start the app:

   ```bash
   python app.py
   ```

6. Open your browser to:

   [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Notes

- The database file is created automatically as `app.db`.
- You do not need to run a separate database server.
- To reset everything, stop the app and delete `app.db`.

## Main pages

- `Dashboard`: revenue, required views, risk alerts, idea ranking, recent performance
- `Videos`: full pipeline list plus video detail command center
- `Ideas`: quick capture and promotion into the production pipeline
- `Deals`: sponsor tracking with linked video relationships
- `Analytics`: average views by concept type
- `Team`: simple team assignment management

## Good next steps for production

- Move the secret key into an environment variable
- Add authentication and permissions
- Swap SQLite for Postgres if you expect multiple users or heavier traffic
- Split the app into API routes plus a frontend app when attaching it to the main production website
- Add YouTube API sync for live views and publish dates
