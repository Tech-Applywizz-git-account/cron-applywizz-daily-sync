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
            .select("jb_id, email, plan_ended") \
            .execute()

        transactions = response.data
        print(f"✅ Found {len(transactions)} transactions in Supabase.")

        # Dictionary to keep the latest end date per JB-ID (fallback to email)
        latest_end_dates = {}

        for trans in transactions:
            jb_id = trans.get("jb_id")
            email = trans.get("email")
            plan_ended = trans.get("plan_ended")
            
            if not plan_ended:
                continue

            plan_ended_date = plan_ended.split("T")[0]
            
            # Use JB-ID as primary key for sync if it exists, otherwise email
            key = jb_id if jb_id else email
            
            if not key:
                continue
            
            if key not in latest_end_dates or plan_ended_date > latest_end_dates[key]['date']:
                latest_end_dates[key] = {
                    'date': plan_ended_date,
                    'jb_id': jb_id,
                    'email': email
                }

        # STEP 2: Sync and Pause based on latest end dates in Render DB
        updated_count = 0
        paused_count = 0
        print(f"🚀 Starting sync for {len(latest_end_dates)} unique users...")

        for key, data in latest_end_dates.items():
            latest_date = data['date']
            jb_id = data['jb_id']
            email = data['email']
            
            print(f"🔄 Syncing end date for: {key} ({latest_date})")
            
            # 1. Sync the end date to karmafy_lead in Render
            # Priority 1: Use JB-ID (as requested)
            rows_affected = 0
            if jb_id:
                cur.execute(
                    'UPDATE public.karmafy_lead SET "endDate" = %s WHERE "apwId" = %s',
                    (latest_date, jb_id)
                )
                rows_affected = cur.rowcount
            
            # Priority 2: Fallback to email if JB-ID didn't match or doesn't exist
            if rows_affected == 0 and email:
                cur.execute(
                    'UPDATE public.karmafy_lead SET "endDate" = %s WHERE LOWER(email) = LOWER(%s)',
                    (latest_date, email)
                )
                rows_affected = cur.rowcount

            if rows_affected > 0:
                updated_count += 1

            # 2. Check for expiration
            if latest_date <= today:
                # Update status to 'paused' if currently 'in progress'
                paused_rows = 0
                if jb_id:
                    cur.execute(
                        'UPDATE public.karmafy_lead SET status = \'paused\' WHERE "apwId" = %s AND status = \'in progress\'',
                        (jb_id,)
                    )
                    paused_rows = cur.rowcount
                
                # Fallback to email if JB-ID update didn't affect any rows
                if paused_rows == 0 and email:
                    cur.execute(
                        "UPDATE public.karmafy_lead SET status = 'paused' WHERE LOWER(email) = LOWER(%s) AND status = 'in progress'",
                        (email,)
                    )
                    paused_rows = cur.rowcount
                    
                if paused_rows > 0:
                    paused_count += paused_rows
                    print(f"⏸️  Paused lead for: {key} (Plan ended on {latest_date})")

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
