import requests
import os
import csv
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
ENTERPRISE = os.getenv("ENTERPRISE")
API_ROOT = "https://api.github.com"
PER_PAGE = 100

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def handle_rate_limit(response):
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
    if remaining == 0:
        current_time = int(time.time())
        sleep_for = max(reset_time - current_time, 1)
        logging.warning(f"Rate limit reached. Sleeping for {sleep_for} seconds until reset.")
        time.sleep(sleep_for + 2)

def fetch_enterprise_teams():
    teams = []
    page = 1
    while True:
        url = f"{API_ROOT}/enterprises/{ENTERPRISE}/teams"
        params = {"per_page": PER_PAGE, "page": page}
        response = requests.get(url, headers=headers, params=params)
        handle_rate_limit(response)
        if response.status_code != 200:
            logging.error(f"Error fetching teams: {response.status_code} - {response.text}")
            break
        data = response.json()
        if not data:
            break
        teams.extend(data)
        if "next" in response.links:
            page += 1
        else:
            break
        time.sleep(1)
    return teams

def fetch_team_metrics(team_slug):
    all_entries = []
    page = 1
    while True:
        url = f"{API_ROOT}/enterprises/{ENTERPRISE}/team/{team_slug}/copilot/metrics"
        params = {"per_page": PER_PAGE, "page": page}
        response = requests.get(url, headers=headers, params=params)
        handle_rate_limit(response)
        if response.status_code == 404:
            logging.error(f"Metrics endpoint not found for team {team_slug} (404).")
            break
        if response.status_code != 200:
            logging.error(f"Error fetching metrics for team {team_slug}: {response.status_code} - {response.text}")
            break
        data = response.json()
        if not data:
            break
        all_entries.extend(data)
        if "next" in response.links:
            page += 1
        else:
            break
        time.sleep(1)
    return all_entries

def write_to_csv(enterprise_id, team_name, entries, csv_writer):
    for entry in entries:
        date = entry.get("date", "N/A")
        copilot_ide_code_completions = entry.get("copilot_ide_code_completions", {})
        total_active_users = entry.get("total_active_users", 0)
        total_chat_engaged_users = entry.get("copilot_dotcom_chat", {}).get("total_engaged_users", 0)
        total_pull_request_engaged_users = entry.get("copilot_dotcom_pull_requests", {}).get("total_engaged_users", 0)

        for editor in copilot_ide_code_completions.get("editors", []):
            editor_name = editor.get("name", "N/A")
            for model in editor.get("models", []):
                model_name = model.get("name", "N/A")
                is_custom_model = model.get("is_custom_model", False)
                total_chats = model.get("total_chats", 0)
                for language in model.get("languages", []):
                    language_name = language.get("name", "N/A")
                    total_engaged_users = language.get("total_engaged_users", 0)
                    total_code_acceptances = language.get("total_code_acceptances", 0)
                    total_code_suggestions = language.get("total_code_suggestions", 0)
                    total_code_lines_accepted = language.get("total_code_lines_accepted", 0)
                    total_code_lines_suggested = language.get("total_code_lines_suggested", 0)

                    csv_writer.writerow([
                        enterprise_id, team_name, date, editor_name, model_name, language_name, total_engaged_users,
                        total_code_acceptances, total_code_suggestions, total_code_lines_accepted,
                        total_code_lines_suggested, total_active_users, total_chat_engaged_users,
                        total_pull_request_engaged_users, '', 0, False, 0, 0
                    ])

        copilot_ide_chat = entry.get("copilot_ide_chat", {})
        for chat_editor in copilot_ide_chat.get("editors", []):
            chat_editor_name = chat_editor.get("name", "N/A")
            for model in chat_editor.get("models", []):
                total_chats = model.get("total_chats", 0)
                is_custom_model = model.get("is_custom_model", False)
                total_chat_copy_events = model.get("total_chat_copy_events", 0)
                total_chat_insertion_events = model.get("total_chat_insertion_events", 0)

                csv_writer.writerow([
                    enterprise_id, team_name, date, '', '', '', 0, 0, 0, 0, 0, total_active_users, total_chat_engaged_users,
                    total_pull_request_engaged_users, chat_editor_name, total_chats, is_custom_model,
                    total_chat_copy_events, total_chat_insertion_events
                ])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    teams = fetch_enterprise_teams()
    if not teams:
        logging.warning("No teams found in enterprise.")
        exit()

    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"copilot_usage_data_teams_{ENTERPRISE}_{current_date}.csv"
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Enterprise", "Team", "date", "editor", "model", "language", "total_engaged_users",
            "total_code_acceptances", "total_code_suggestions", "total_code_lines_accepted",
            "total_code_lines_suggested", "total_active_users", "total_chat_dotcom_engaged_users",
            "total_pull_request_dotcom_engaged_users", "chat_editor_name",
            "total_chats", "is_custom_model", "total_chat_copy_events", "total_chat_insertion_events"
        ])
        for team in teams:
            team_name = team.get("name", "N/A")
            team_slug = team.get("slug")
            if not team_slug:
                continue
            logging.info(f"Fetching metrics for team: {team_name} ({team_slug})")
            team_metrics = fetch_team_metrics(team_slug)
            write_to_csv(ENTERPRISE, team_name, team_metrics, writer)

    logging.info(f"All team data written to {output_file}")
