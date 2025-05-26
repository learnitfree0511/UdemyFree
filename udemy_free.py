import requests
from bs4 import BeautifulSoup
import time
import random
from fake_useragent import UserAgent
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import pytz
from airtable import Airtable

# --- Cấu hình Logging ---
vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('{asctime} - {levelname} - {message}', datefmt='%Y-%m-%d %H:%M:%S', style='{')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# --- Cấu hình Airtable ---
# CÁC THAM SỐ AIRTABLE ĐƯỢC CẬP NHẬT TRỰC TIẾP VÀO ĐÂY
# ----------------------------------------------------------
# ***** CẢNH BÁO BẢO MẬT *****
# KHÔNG chia sẻ file này công khai khi chứa khóa API thực tế.
# Cân nhắc sử dụng biến môi trường cho các thông tin nhạy cảm.
# ----------------------------------------------------------
AIRTABLE_API_KEY = "patGxDaOzzFPqjQq6.6169f43944d6629aa1266949ea269666fdc0fc6d7be6bf345df3099a945056b6"
AIRTABLE_BASE_ID = "apptYwt65GkOcmP8X"
AIRTABLE_TABLE_NAME = "khoa_hoc_udemy_free" # For courses
AIRTABLE_LOG_TABLE_NAME = "Log_Crawler"     # For logs
# ----------------------------------------------------------

# --- Airtable Log Handler ---
class AirtableLogHandler(logging.Handler):
    def __init__(self, api_key, base_id, table_name):
        super().__init__()
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.airtable = None

        print(f"AirtableLogHandler: Initializing for table '{table_name}' with API Key: {'SET' if api_key else 'NOT SET'}, Base ID: {base_id}")

        if not self.api_key or not self.base_id or not self.table_name:
            print(f"AirtableLogHandler INIT ERROR: Missing API key ('{self.api_key}'), Base ID ('{self.base_id}'), or Table Name ('{self.table_name}') for logs. Handler will not connect.")
            return
        try:
            self.airtable = Airtable(self.base_id, self.table_name, api_key=self.api_key)
            print(f"AirtableLogHandler INIT SUCCESS: Attempted connection to Airtable table '{self.table_name}' for logging. Handler object: {self.airtable}")
        except Exception as e:
            self.airtable = None
            print(f"AirtableLogHandler INIT ERROR: Failed to connect to Airtable table '{self.table_name}'. Error: {e}")

    def emit(self, record):
        if not self.airtable:
            return
        try:
            details_for_airtable = ""
            if record.exc_text:
                details_for_airtable = record.exc_text
            elif record.exc_info:
                details_for_airtable = self.formatException(record.exc_info)

            log_entry = {
                'Timestamp': datetime.fromtimestamp(record.created, tz=vietnam_tz).isoformat(),
                'Level': record.levelname,
                'Message': record.getMessage(),
                'Details': details_for_airtable,
                'Script_Name': record.name
            }
            self.airtable.insert(log_entry)
        except Exception as e:
            print(f"AirtableLogHandler (emit) ERROR: Could not log to Airtable table '{self.table_name}'. Record: '{record.getMessage()}'. Error: {e}")

# Khởi tạo AirtableLogHandler
airtable_handler_initialized = False
print(f"DEBUG: Pre-Handler Init - AIRTABLE_API_KEY: {'SET' if AIRTABLE_API_KEY else 'NOT SET'}")
print(f"DEBUG: Pre-Handler Init - AIRTABLE_BASE_ID: {AIRTABLE_BASE_ID}")
print(f"DEBUG: Pre-Handler Init - AIRTABLE_LOG_TABLE_NAME: {AIRTABLE_LOG_TABLE_NAME}")

if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_LOG_TABLE_NAME:
    print("DEBUG: Attempting to initialize AirtableLogHandler...")
    try:
        airtable_handler = AirtableLogHandler(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_LOG_TABLE_NAME)
        if airtable_handler.airtable:
            airtable_handler.setFormatter(logging.Formatter('{message}', style='{'))
            logger.addHandler(airtable_handler)
            airtable_handler_initialized = True
            print("DEBUG: AirtableLogHandler initialized and added to logger.")
        else:
            logger.warning("AirtableLogHandler object was not successfully created (self.airtable is None post-init). Handler not added.")
            print("DEBUG: AirtableLogHandler self.airtable is None post-init. Handler not added.")
    except Exception as e:
        logger.error(f"Exception during AirtableLogHandler setup: {e}", exc_info=True)
        print(f"DEBUG: Exception during AirtableLogHandler setup: {e}")
else:
    logger.warning("AirtableLogHandler not initialized due to missing Airtable credentials (API_KEY, BASE_ID, or LOG_TABLE_NAME).")
    print("DEBUG: AirtableLogHandler not initialized due to missing credentials.")


# --- Crawler Configuration ---
BASE_URL = "https://www.discudemy.com"
COURSES_URL = f"{BASE_URL}/language/english"

def get_page(url, max_attempts=3):
    logger.debug(f"Attempting to get page: {url}")
    for attempt in range(max_attempts):
        try:
            time.sleep(random.uniform(2.0, 4.0))
            ua = UserAgent()
            response = requests.get(url, headers={"User-Agent": ua.random}, timeout=20)
            response.raise_for_status()
           ###### Toannx bo log len db logger.debug(f"Successfully retrieved page: {url}")
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{max_attempts} failed to get page {url}: {e}")
            if attempt == max_attempts - 1:
                logger.error(f"All {max_attempts} attempts failed for page {url}.")
                return None
            time.sleep(random.uniform(3.0, 7.0))
    return None

def get_go_link(course_url):
    logger.debug(f"Getting go_link for: {course_url}")
    # ... (Nội dung hàm get_go_link giữ nguyên như file trước của bạn) ...
    soup = get_page(course_url)
    if not soup:
        logger.warning(f"Could not get page content for {course_url} to find go_link.")
        return None
    btn = soup.find("a", class_="ui huge green icon button")
    if btn and btn.get("href") and "/go/" in btn.get("href", ""):
        go_link_path = btn["href"]
        logger.debug(f"Found go_link button: {go_link_path}")
        return urljoin(BASE_URL, go_link_path)
    go_links_on_page = soup.find_all("a", href=lambda x: x and "/go/" in x)
    if go_links_on_page:
        for glink in go_links_on_page:
            if 'button' in glink.get('class', []) or glink.find_parent(class_=['segment', 'card']):
                go_link_path = glink['href']
                logger.debug(f"Found alternative go_link: {go_link_path}")
                return urljoin(BASE_URL, go_link_path)
        go_link_path = go_links_on_page[0]['href']
        logger.debug(f"Found a generic go_link on page: {go_link_path}")
        return urljoin(BASE_URL, go_link_path)
    parsed_url = urlparse(course_url)
    path_parts = parsed_url.path.strip("/").split("/")
    course_slug = path_parts[-1] if path_parts else ""
    if course_slug and not (course_slug in ['english', 'all'] or len(path_parts) <=2 ):
        go_link_attempt = f"{BASE_URL}/go/{course_slug}"
        logger.debug(f"Trying constructed go_link: {go_link_attempt}")
        return go_link_attempt
    logger.warning(f"Could not determine go_link for {course_url}")
    return None

def get_udemy_link(go_url):
    logger.debug(f"Getting Udemy link from go_url: {go_url}")
    # ... (Nội dung hàm get_udemy_link giữ nguyên như file trước của bạn) ...
    if not go_url:
        logger.warning("go_url is None, cannot get Udemy link.")
        return None, None, None
    if "udemy.com/course/" in go_url:
        logger.info(f"Provided go_url is already a Udemy link: {go_url}")
        clean_link = go_url.split("?")[0]
        code = go_url.split("couponCode=")[-1].split("&")[0] if "couponCode=" in go_url else "N/A"
        return clean_link, go_url, code
    soup = get_page(go_url)
    if not soup:
        logger.warning(f"Could not get page content for {go_url} to find Udemy link.")
        return None, None, None
    meta_refresh = soup.find("meta", attrs={"http-equiv": "refresh"})
    if meta_refresh and meta_refresh.get("content"):
        content = meta_refresh["content"]
        if "url=" in content.lower():
            udemy_target_link = content.lower().split("url=")[-1]
            if "udemy.com/course/" in udemy_target_link:
                logger.debug(f"Found Udemy link via meta refresh: {udemy_target_link}")
                clean_link = udemy_target_link.split("?")[0]
                code = udemy_target_link.split("couponCode=")[-1].split("&")[0] if "couponCode=" in udemy_target_link else "N/A"
                return clean_link, udemy_target_link, code
    all_links = soup.find_all("a", href=lambda x: x and "udemy.com/course/" in x)
    for link_tag in all_links:
        href = link_tag.get("href", "")
        parent = link_tag.find_parent()
        context_text = parent.get_text(strip=True) if parent else ""
        if "Course Coupon" in context_text or "Coupon" in context_text or "Skip Ad" in context_text or "Go to Course" in context_text:
            logger.debug(f"Found Udemy link with context '{context_text}': {href}")
            clean_link = href.split("?")[0]
            code = href.split("couponCode=")[-1].split("&")[0] if "couponCode=" in href else "N/A"
            return clean_link, href, code
    if all_links:
        href = all_links[0].get("href", "")
        logger.debug(f"Found a generic Udemy link on page: {href}")
        clean_link = href.split("?")[0]
        code = href.split("couponCode=")[-1].split("&")[0] if "couponCode=" in href else "N/A"
        return clean_link, href, code
    logger.warning(f"Could not find Udemy link on {go_url}")
    return None, None, None

def crawl_courses():
    logger.info(f"Starting course crawl from: {COURSES_URL}")
    soup = get_page(COURSES_URL)
    if not soup:
        logger.error("Failed to retrieve main courses page. Aborting crawl.")
        return []

    cards = soup.find_all("div", class_="card")
    if not cards:
        cards = soup.find_all("section", class_="card")

    logger.info(f"Found {len(cards)} potential course cards.")
    results = []
    processed_links = set()

    for card_idx, card in enumerate(cards):
        logger.info(f"Processing card {card_idx + 1}/{len(cards)}")
        title = "Unknown Title"
        try:
            header = card.find("a", class_="card-header")
            if not header or not header.get("href"):
                logger.warning(f"Card {card_idx + 1} missing header or href. Skipping.")
                continue

            title = header.get_text(strip=True)
            course_page_link = urljoin(BASE_URL, header["href"])

            if course_page_link in processed_links:
                logger.info(f"Course link {course_page_link} already processed. Skipping.")
                continue
            processed_links.add(course_page_link)

            desc_tag = card.find("div", class_="description")
            desc = desc_tag.get_text(strip=True) if desc_tag else "N/A"

            img_tag = card.find("img", class_="card-img-top")
            if not img_tag:
                img_tag = card.find("amp-img")
            img_src = img_tag["src"] if img_tag and img_tag.get("src") else "N/A"
            if img_src.startswith("//"):
                img_src = "https:" + img_src
            elif img_src != 'N/A' and img_src.startswith("/"):
                img_src = urljoin(BASE_URL, img_src)

            logger.info(f"Course: {title} | Page: {course_page_link}")

            go_link = get_go_link(course_page_link)
            if not go_link:
                logger.warning(f"No go_link found for {title}. Skipping.")
                continue
            logger.info(f"Go Link: {go_link}")

            udemy_clean, udemy_full, code = get_udemy_link(go_link)
            if not udemy_clean:
                logger.warning(f"No Udemy link found for {title} from go_link {go_link}. Skipping.")
                continue
            logger.info(f"Udemy Link: {udemy_clean} | Full: {udemy_full} | Code: {code}")

            # **Sử dụng key tiếng Việt để khớp với Airtable**
            results.append({
                'Tiêu đề': title,
                'Mô tả': desc,
                'Ảnh': img_src,
                'Link học miễn phí': udemy_clean,
                'Link Udemy (có CODE)': udemy_full,
                'CODE': code,
            })
            logger.info(f"Successfully processed and added: {title}")
        except Exception as e:
            logger.error(f"Error processing a card for course '{title}': {e}", exc_info=True)
            continue
    logger.info(f"Finished processing all cards. Total courses collected: {len(results)}")
    return results

def delete_all_records_from_table(base_id, table_name, api_key):
    """Xóa tất cả các bản ghi khỏi một bảng Airtable cụ thể."""
    logger.info(f"Attempting to delete all records from table: {table_name} in base: {base_id}")
    try:
        table_to_clear = Airtable(base_id, table_name, api_key=api_key)
        all_records = table_to_clear.get_all(fields=[]) # Chỉ lấy ID cho hiệu quả
        
        if not all_records:
            logger.info(f"Table '{table_name}' is already empty. No records to delete.")
            return True

        record_ids_to_delete = [record['id'] for record in all_records]
        logger.info(f"Found {len(record_ids_to_delete)} records to delete from '{table_name}'.")

        # Airtable API cho phép xóa tối đa 10 bản ghi mỗi lần gọi batch_delete
        # Thư viện airtable-python-wrapper tự động xử lý việc chia nhỏ này.
        if record_ids_to_delete:
            table_to_clear.batch_delete(record_ids_to_delete)
            logger.info(f"Successfully deleted {len(record_ids_to_delete)} records from '{table_name}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to delete records from table '{table_name}'. Error: {e}", exc_info=True)
        return False

def save_to_airtable(courses_data):
    if not courses_data:
        logger.info("No courses to save to Airtable.")
        return

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
        logger.error("Cannot save to Airtable due to missing API Key, Base ID, or Table Name in script configuration.")
        return

    try:
        airtable_courses_table = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, api_key=AIRTABLE_API_KEY)
        logger.info(f"Attempting to save {len(courses_data)} new courses to Airtable table: {AIRTABLE_TABLE_NAME}")
        saved_count = 0
        skipped_count = 0 # Sẽ không có skipped nếu đã xóa hết, trừ khi có trùng link trong batch mới

        for course in courses_data:
            try:
                # Dữ liệu đã có key tiếng Việt từ hàm crawl_courses
                # Thêm Scrape Date
                record_to_insert = course.copy() # Tạo bản sao để không thay đổi dict gốc
                record_to_insert['Scrape Date'] = datetime.now(vietnam_tz).isoformat()
                
                # Kiểm tra trùng lặp (nếu cần thiết, mặc dù đã xóa hết trước đó)
                # Nếu bạn chắc chắn bảng trống, có thể bỏ qua bước kiểm tra trùng lặp này để tăng tốc.
                # Tuy nhiên, để an toàn, vẫn giữ lại phòng trường hợp có link trùng trong batch mới.
                link_hoc_mien_phi_value = record_to_insert.get('Link học miễn phí', '')
                if link_hoc_mien_phi_value:
                    link_hoc_mien_phi_value = link_hoc_mien_phi_value.replace("'", "''")
                else:
                    link_hoc_mien_phi_value = ""

                if not link_hoc_mien_phi_value:
                    search_formula = "FALSE()"
                else:
                    search_formula = f"{{Link học miễn phí}} = '{link_hoc_mien_phi_value}'"
                
                existing_records = []
                if record_to_insert.get('Link học miễn phí') and link_hoc_mien_phi_value:
                    existing_records = airtable_courses_table.get_all(formula=search_formula, max_records=1)

                if existing_records:
                    logger.info(f"Course '{record_to_insert.get('Tiêu đề')}' (Link: {record_to_insert.get('Link học miễn phí')}) found again in current batch or delete failed. Skipping.")
                    skipped_count += 1
                else:
                    airtable_courses_table.insert(record_to_insert)
                    logger.info(f"Successfully saved course to Airtable: {record_to_insert.get('Tiêu đề')}")
                    saved_count +=1
            except TypeError as te:
                 logger.error(f"TypeError while processing course '{course.get('Tiêu đề', 'N/A')}' for Airtable. Link might be None. Error: {te}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing or saving course '{course.get('Tiêu đề', 'N/A')}' to Airtable: {e}", exc_info=True)
        
        logger.info(f"Finished saving courses to Airtable. Saved: {saved_count}, Skipped: {skipped_count}.")

    except Exception as e:
        logger.error(f"Failed to connect to Airtable or critical error during saving process for table {AIRTABLE_TABLE_NAME}: {e}", exc_info=True)


if __name__ == "__main__":
    print("DEBUG: Script __main__ execution started.")
    print(f"DEBUG: AirtableLogHandler was initialized according to script logic: {airtable_handler_initialized}")

    if airtable_handler_initialized:
        logger.info("Script execution started via logger. Airtable logging should be active.")
    else:
        logger.info("Script execution started via logger. Airtable logging NOT active (check DEBUG prints above for handler init status).")
    
    try:
        logger.info(f"Using Airtable Base ID: {AIRTABLE_BASE_ID}")
        logger.info(f"Target courses table: {AIRTABLE_TABLE_NAME}")
        logger.info(f"Target logs table: {AIRTABLE_LOG_TABLE_NAME}")

        # Xóa tất cả bản ghi cũ trong bảng khóa học

        # --- MỚI: XÓA TẤT CẢ LOGS TRƯỚC KHI TIẾN HÀNH ---
        logger.info(f"Attempting to clear old logs from table '{AIRTABLE_LOG_TABLE_NAME}'...")
        delete_all_records_from_table(AIRTABLE_BASE_ID, AIRTABLE_LOG_TABLE_NAME, AIRTABLE_API_KEY)
        # --- HẾT PHẦN MỚI ---

        logger.info(f"Attempting to clear old data from table '{AIRTABLE_TABLE_NAME}'...")
        delete_successful = delete_all_records_from_table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_API_KEY)
        if not delete_successful:
            logger.error(f"Halting script because old data could not be deleted from '{AIRTABLE_TABLE_NAME}'. Please check logs.")
        else:
            logger.info("Old data cleared (or table was already empty). Proceeding to crawl new courses.")
            crawled_courses_data = crawl_courses()
            
            if crawled_courses_data:
                save_to_airtable(crawled_courses_data)
            else:
                logger.info("No courses were crawled, skipping save to Airtable.")
            
        logger.info("Script execution finished.")
    except Exception as main_exception:
        logger.error(f"A critical error occurred in __main__: {main_exception}", exc_info=True)
    finally:
        print("DEBUG: Script __main__ execution finished block.")