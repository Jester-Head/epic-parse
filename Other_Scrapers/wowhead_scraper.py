from playwright.sync_api import sync_playwright
import json


def scrape_wowhead_database_categories():
    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Use headless=True once you're sure it's working
        page = browser.new_page()
        page.goto("https://www.wowhead.com/database")
        page.wait_for_timeout(5000)  # let JS render menus

        # All top-level sections (like Items, Spells, Quests, etc.)
        sections = page.query_selector_all('div.menu-map-section')  # top-level categories

        for section in sections:
            try:
                a_tag = section.query_selector("a")
                if a_tag is not None:
                    category_title = a_tag.inner_text().strip().lower()
                else:
                    print("Warning: No <a> tag found in section.")
                    continue

                sub_items = section.query_selector_all("ul > li > a")  # dropdown items
                if sub_items:
                    subcategories = [s.inner_text().strip().lower() for s in sub_items]
                    results[category_title] = subcategories
                else:
                    results[category_title] = []
            except Exception as e:
                print(f"Error processing section: {e}")

        browser.close()

    return results


# Run and save
if __name__ == "__main__":
    data = scrape_wowhead_database_categories()

    # Save to file
    with open("wowhead_database_categories.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print("✅ Categories scraped and saved to wowhead_database_categories.json")
