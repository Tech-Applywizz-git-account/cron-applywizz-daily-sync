import os
import datetime
import psycopg2
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables for local testing
load_dotenv()

# --- INITIALIZE SUPABASE (For transactions) ---
supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("VITE_SUPABASE_SERVICE_ROLE_KEY")

if not supabase_url or not supabase_key:
    print("❌ Missing Supabase configuration.")
    exit(1)

supabase: Client = create_client(supabase_url, supabase_key)

# --- INITIALIZE RENDER DB (For karmafy_lead) ---
render_db_url = os.environ.get("RENDER_DB_URL")

if not render_db_url:
    print("❌ Missing RENDER_DB_URL configuration.")
    exit(1)

def sync_and_expire_leads():
    print("⏰ Starting Daily Cron Job (Hybrid DB): Syncing End Dates and Updating Statuses...")
    
    # Get today's date in YYYY-MM-DD
    today = datetime.date.today().isoformat()
    print(f"📅 Today's Date: {today}")

    # Connect to Render PostgreSQL
    try:
        conn = psycopg2.connect(render_db_url)
        cur = conn.cursor()
        print("✅ Connected to Render Database.")
    except Exception as e:
        print(f"❌ Failed to connect to Render Database: {e}")
        exit(1)

    try:
        # STEP 1: Fetch transactions from Supabase
        print("🔄 Fetching transactions from Supabase (jobboard_transactions)...")
        response = supabase.table("jobboard_transactions") \
            .select("email, plan_ended") \
            .execute()

        transactions = response.data
        print(f"✅ Found {len(transactions)} transactions in Supabase.")

        # Dictionary to keep the latest end date per email
        latest_end_dates = {}

        for trans in transactions:
            email = trans.get("email")
            plan_ended = trans.get("plan_ended")
            
            if not email or not plan_ended:
                continue

            plan_ended_date = plan_ended.split("T")[0]
            
            if email not in latest_end_dates or plan_ended_date > latest_end_dates[email]:
                latest_end_dates[email] = plan_ended_date

        # STEP 2: Sync and Pause based on latest end dates in Render DB
        updated_count = 0
        paused_count = 0
        print(f"🚀 Starting sync for {len(latest_end_dates)} unique users...")

        for email, latest_date in latest_end_dates.items():
            print(f"🔄 Syncing end date for: {email} ({latest_date})")
            # 1. Sync the end date to karmafy_lead in Render
            # The table name is karmafy_lead, column is "endDate"
            cur.execute(
                'UPDATE public.karmafy_lead SET "endDate" = %s WHERE email = %s',
                (latest_date, email)
            )
            if cur.rowcount > 0:
                updated_count += 1

            # 2. Check for expiration
            if latest_date <= today:
                # Update status to 'paused' if currently 'in progress'
                cur.execute(
                    "UPDATE public.karmafy_lead SET status = 'paused' WHERE email = %s AND status = 'in progress'",
                    (email,)
                )
                if cur.rowcount > 0:
                    paused_count += cur.rowcount
                    print(f"⏸️  Paused lead for: {email} (Plan ended on {latest_date})")

        conn.commit()

        print(f"\n📊 Summary:")
        print(f"- Unique users processed from Supabase: {len(latest_end_dates)}")
        print(f"- Render records updated with end date: {updated_count}")
        print(f"- Leads moved to 'paused' state in Render: {paused_count}")
        print("🏁 Cron job completed successfully.")

    except Exception as e:
        print(f"❌ Cron Job Failed: {str(e)}")
        conn.rollback()
        exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    sync_and_expire_leads()
