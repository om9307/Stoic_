import scrapy
import os
import csv
import json
import re
from datetime import datetime, date
from scrapy.crawler import CrawlerProcess
class BankrateRatesSpider(scrapy.Spider):
    name = "bankrate_rates"
    allowed_domains = ["bankrate.com"]
    start_urls = ["https://www.bankrate.com/mortgages/mortgage-rates/"]
    def __init__(self):
        super().__init__()
        # File paths
        self.csv_path = "bankrate_rates_history.csv"   # Cumulative CSV history
        self.json_path = "bankrate_loans.json"         # Daily snapshot JSON
        # CSV headers matching your desired data structure
        self.fieldnames = [
            "loan_product",
            "interest_rate",
            "apr_percent",
            "loan_term_years",
            "lender_name",
            "updated_date"
        ]
        self.scraped_data = []
    def parse(self, response):
        # Extract the "Rates as of ..." date text
        raw_date = response.css('p.mb-0::text').re_first(r'Rates as of (.*)')
        if raw_date:
            try:
                scraped_date = datetime.strptime(raw_date.strip(), "%A, %B %d, %Y at %I:%M %p").date()
            except Exception:
                self.logger.warning("Failed to parse 'Rates as of' date, defaulting to today's date.")
                scraped_date = date.today()
        else:
            self.logger.warning("No 'Rates as of' text found, defaulting to today's date.")
            scraped_date = date.today()
        today = date.today()
        if scraped_date < today:
            self.logger.info(f":hourglass_flowing_sand: Skipping past data (Rates as of {scraped_date} < today {today}).")
            return
        updated_date = scraped_date.isoformat()
        # Load existing entries from CSV to avoid duplicates
        existing_keys = set()
        if os.path.exists(self.csv_path):
            with open(self.csv_path, newline='', encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_keys.add((row.get("loan_product"), row.get("updated_date")))
        # Select mortgage product rows from the Purchase Rates table
        rows = response.css('div[aria-labelledby="purchase-0"] table tbody tr')
        for row in rows:
            product_full = row.css('th a::text').get(default='').strip()
            interest_rate = row.css('td:nth-of-type(1)::text').get(default='').strip()
            apr = row.css('td:nth-of-type(2)::text').get(default='').strip()
            if not (product_full and interest_rate and apr):
                continue  # Skip rows with missing data
            # Extract loan term years from product name, e.g., "30-Year Fixed Rate" -> 30
            match = re.search(r'(\d+)-Year', product_full)
            loan_term = int(match.group(1)) if match else None
            key = (product_full, updated_date)
            if key in existing_keys:
                continue  # Skip duplicates
            item = {
                "loan_product": product_full,
                "interest_rate": interest_rate,
                "apr_percent": apr,
                "loan_term_years": loan_term,
                "lender_name": "Bankrate",
                "updated_date": updated_date
            }
            self.scraped_data.append(item)
            yield item
    def closed(self, reason):
        if not self.scraped_data:
            print(":warning: No new data scraped. Nothing saved.")
            return
        # Save daily snapshot JSON file (overwrites daily)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self.scraped_data, f, indent=2)
        print(f":white_check_mark: Snapshot JSON saved to {self.json_path}")
        # Append new unique rows to CSV file
        write_header = not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerows(self.scraped_data)
        print(f":white_check_mark: Appended {len(self.scraped_data)} new row(s) to {self.csv_path}")
if __name__ == "__main__":
    process = CrawlerProcess(settings={
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "LOG_LEVEL": "INFO"
    })
    process.crawl(BankrateRatesSpider)
    process.start()