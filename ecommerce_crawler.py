import re
import time
import threading
import hashlib
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# BATCH_SIZE = 50
THREAD_POOL = 5  # no of thread workers
PROCESS_POOL = 2  # no of process workers
MAX_DEPTH = 1  # max recursive depth for a domain
PRODUCT_PATTERNS = [
    r"/product/",
    r"/products/",
    r"/item/",
    r"/p/",
    r"/shop/",
    r"/goods/",
    r"/fashion/",
]

HASH_TABLE = {}  # to Maintain url hash codes


_local_thread = threading.local()


def get_driver():
    """
    Method to get Chrome Driver
    """
    if not hasattr(_local_thread, "driver"):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disble-dev-shm-usage")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        _local_thread.driver = driver
    return _local_thread.driver


def scroll_page(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for the page to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def click_load_more_buttons(driver):
    try:
        while True:
            load_more_button = driver.find_element_by_xpath(
                "//button[contains(text(), 'Load More')]"
            )
            if load_more_button:
                load_more_button.click()
                time.sleep(2)  # Wait for the page to load
            else:
                break
    except Exception as e:
        print(f"No more 'Load More' buttons found: {e}")


def downlaod_page(url):
    """Method to download Page"""

    driver = get_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href]"))
        )
        scroll_page(driver)
        click_load_more_buttons(driver)
        page_source = driver.page_source
        return url, page_source
    except Exception as e:
        print("error happened", e)
        return url, None


def hash_url(url: str):
    """Method to get hash for a url string"""
    value = hashlib.sha256(url.encode()).hexdigest()
    HASH_TABLE[value] = url
    return value


def get_base_url(url):
    """Method to get Base URL"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def extract_links(url, html):
    """Method to extract link from a page"""
    try:
        soup = BeautifulSoup(html, "lxml")
        links = set()
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.startswith("/"):
                print("*", end="")
                domain = get_base_url(url)
                href = f"{domain}{href}"
            links.add(href)
        return url, links
    except Exception as e:
        return url, set()


def extract_product_urls(file_path, html):
    "Method to to extract Product URLs"
    try:
        soup = BeautifulSoup(html, "lxml")
        product_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(re.search(pattern, href) for pattern in PRODUCT_PATTERNS):
                if href.startswith("/"):
                    print(".", end="")
                    domain = file_path
                    domain = get_base_url(domain)
                    href = f"{domain}{href}"
                product_urls.add(href)
        return file_path, product_urls
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return file_path, set()


def process_link(link):
    """method to handle a given link from downloading to parsing"""
    url, html = downlaod_page(link)
    if not html:
        return set(), set()
    path, product_urls = extract_product_urls(url, html)
    path, links = extract_links(url, html)
    return product_urls, links


def process_domain(domain, max_depth=MAX_DEPTH):
    """Method to process a single domain"""
    links_hash = set([hash_url(domain)])
    processed = set()
    product_urls = set()
    depth = 0
    while links_hash and depth < max_depth:
        links = list(map(lambda x: HASH_TABLE[x], links_hash))
        with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
            results = list(executor.map(process_link, links))

        # time.sleep(2)

        processed = processed.union(links_hash)
        new_product_urls = set()
        new_links = set()
        for result in results:
            p_urls = set(map(hash_url, result[0]))
            new_product_urls = new_product_urls.union(p_urls)
            n_links = set(map(hash_url, result[1]))
            new_links = new_links.union(n_links)

        links_hash = new_links.difference(new_product_urls).difference(processed)
        product_urls = product_urls.union(new_product_urls)
        depth += 1
        print(depth)

        with open("product_urls.log", "a", encoding="UTF-8") as file:
            file.write("\n".join(list(map(lambda x: HASH_TABLE[x], new_product_urls))))
    return {
        "domain": domain,
        "products": list(map(lambda x: HASH_TABLE[x], product_urls)),
    }


def process_pages(domains):
    """Method to process all domains"""
    with ProcessPoolExecutor(PROCESS_POOL) as executor:
        futures = [executor.submit(process_domain, domain) for domain in domains]
        results = [future.result() for future in futures]
    return results


def main():
    """Main function"""
    domains = [
        "https://www.flipkart.com/",
        "https://www.snitch.co.in/",
        # "https://www.only.in/fashion",
        "https://styleunion.in/",
    ]
    product_urls = process_pages(domains)
    with open("product_urls.json", "w", encoding="UTF-8") as file:
        file.write(json.dumps(product_urls))


if __name__ == "__main__":
    main()
