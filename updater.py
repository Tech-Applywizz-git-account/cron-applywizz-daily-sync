import os
import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables for local testing
load_dotenv()

# Initialize Supabase
supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("VITE_SUPABASE_SERVICE_ROLE_KEY")

if not supabase_url or not supabase_key:
    print("❌ Missing Supabase configuration. Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.")
    exit(1)

supabase: Client = create_client(supabase_url, supabase_key)

def sync_and_expire_leads():
    print("⏰ Starting Daily Cron Job (Python): Syncing End Dates and Updating Statuses...")
    
    # Get today's date in YYYY-MM-DD
    today = datetime.date.today().isoformat()
    print(f"📅 Today's Date: {today}")

    try:
        # STEP 1: Fetch transactions
        print("🔄 Fetching transactions from jobboard_transactions...")
        response = supabase.table("jobboard_transactions") \
            .select("email, plan_ended, payment_status") \
            .execute()

        transactions = response.data
        print(f"✅ Found {len(transactions)} transactions to process.")

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

        # STEP 2: Sync and Pause based on latest end dates
        updated_count = 0
        paused_count = 0

        for email, latest_date in latest_end_dates.items():
            # 1. Sync the end date to karmafy_lead
            sync_response = supabase.table("karmafy_lead") \
                .update({"endDate": latest_date}) \
                .eq("email", email) \
                .execute()

            if sync_response.data:
                updated_count += 1

            # 2. Check for expiration
            if latest_date <= today:
                pause_response = supabase.table("karmafy_lead") \
                    .update({"status": "paused"}) \
                    .eq("email", email) \
                    .eq("status", "in progress") \
                    .execute()

                if pause_response.data:
                    paused_count += len(pause_response.data)
                    print(f"⏸️  Paused lead for: {email} (Plan ended on {latest_date})")

        print(f"\n📊 Summary:")
        print(f"- Unique users processed: {len(latest_end_dates)}")
        print(f"- karmafy_lead records updated with end date: {updated_count}")
        print(f"- Leads moved to 'paused' state: {paused_count}")
        print("🏁 Cron job completed successfully.")

    except Exception as e:
        print(f"❌ Cron Job Failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    sync_and_expire_leads()
