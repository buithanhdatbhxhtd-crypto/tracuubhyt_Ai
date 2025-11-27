import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time
import os
import zipfile
import glob
from google.cloud import firestore
from google.oauth2 import service_account
import json

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Chuyên Nghiệp",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 API KEY AI (DÁN KEY CỦA BẠN VÀO DƯỚI)
# ==============================================================================
HARDCODED_API_KEY = "AIzaSyCw8kpB4mr_rw9IAh3-UOoaQfB8y_x16NE" 

# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

# --- 1. KẾT NỐI DATABASE ĐÁM MÂY (FIREBASE) ---
@st.cache_resource
def get_firestore_db():
    """Kết nối đến Google Firestore qua Secrets"""
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            db = firestore.Client(credentials=creds, project=key_dict["project_id"])
            return db
        else:
            return None
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Database Online: {e}")
        return None

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- QUẢN LÝ USER (CLOUD) ---
def create_user(username, password, role):
    db = get_firestore_db()
    if not db: return False
    
    doc_ref = db.collection("users").document(username)
    if doc_ref.get().exists:
        return False 
    
    doc_ref.set({
        "password": make_hashes(password),
        "role": role,
        "created_at": datetime.datetime.now()
    })
    return True

def verify_login(username, password):
    db = get_firestore_db()
    if not db: return None
    
    doc_ref = db.collection("users").document(username)
    doc = doc_ref.get()
    
    if doc.exists:
        user_data = doc.to_dict()
        if user_data["password"] == make_hashes(password):
            return user_data["role"]
    return None

def delete_user_cloud(username):
    db = get_firestore_db()
    if db:
        db.collection("users").document(username).delete()
        return True
    return False

def get_all_users():
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    users = []
    try:
        docs = db.collection("users").stream()
        for doc in docs:
            u = doc.to_dict()
            u['username'] = doc.id
            users.append(u)
    except: pass
    return pd.DataFrame(users)

# --- QUẢN LÝ LOGS (CLOUD) - CẬP NHẬT GIỜ VIỆT NAM ---
def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            # Lấy giờ UTC hiện tại + 7 tiếng = Giờ Việt Nam
            vn_timezone = datetime.timezone(datetime.timedelta(hours=7))
            now_vn = datetime.datetime.now(vn_timezone)
            timestamp_str = now_vn.strftime("%Y-%m-%d %H:%M:%S")
            
            db.collection("logs").add({
                "timestamp": timestamp_str, # Lưu dạng chuỗi để dễ đọc
                "sort_time": firestore.SERVER_TIMESTAMP, # Lưu dạng time để sort
                "username": username,
                "action": action,
                "details": str(details)
            })
    except Exception as e: 
        print(f"Log Error: {e}")

def get_logs(limit=100):
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        # Sắp xếp theo thời gian thực (mới nhất lên đầu)
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        
        data = []
        for doc in logs_ref.stream():
            d = doc.to_dict()
            # Chỉ lấy các cột cần thiết để hiển thị
            row = {
                "Thời gian (VN)": d.get("timestamp", ""),
                "Người dùng": d.get("username", ""),
                "Hành động": d.get("action", ""),
                "Chi tiết": d.get("details", "")
            }
            data.append(row)
            
        return pd.DataFrame(data)
    except Exception as e: 
        st.error(f"Lỗi tải logs: {e}")
        return pd.DataFrame()

# --- KHỞI TẠO ADMIN ---
def init_cloud_admin():
    if "admin_checked" not in st.session_state:
        if verify_login("admin", "admin123") is None:
            create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

# --- 2. HỆ THỐNG AI ---
def configure_ai():
    api_key = HARDCODED_API_KEY
    if not api_key: api_key = st.session_state.get('user_api_key', '')
    if not api_key: api_key = st.secrets.get("GOOGLE_API_KEY", "")
    if api_key: 
        genai.configure(api_key=api_key)
        return True
    return False

def get_ai_response(prompt, role_desc="", stream=False):
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."
    models = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro"]
    full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
    
    for m in models:
        try:
            model = genai.GenerativeModel(m)
            if stream: return model.generate_content(full_prompt, stream=True)
            return model.generate_content(full_prompt).text
        except: continue
    return "⚠️ Hệ thống AI đang bận."

# --- 3. XỬ LÝ DỮ LIỆU (LOCAL SQLITE) ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db()
            res = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'").fetchone()
            conn.close()
            if res and res[0] > 0: return True, "Sẵn sàng"
        except: os.remove(DB_FILE)

    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Đang nối dữ liệu ({len(parts)} phần)...")
        try:
            full_zip = "bhxh_data_full.zip"
            with open(full_zip, 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            msg.info("📦 Đang giải nén...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            if os.path.exists(full_zip): os.remove(full_zip)
            msg.success("✅ Xong!"); time.sleep(0.5); msg.empty(); return True, "Restored"
        except Exception as e: return False, f"Lỗi file: {str(e)}"

    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Thiếu dữ liệu (bhxh_data.zip)"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel. Nên dùng tool chia nhỏ file.")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang xử lý..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(40)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        for col in df.columns:
            if col != 'master_search_idx': df[f'idx_{col}'] = df[col].apply(clean_text)
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        bar.progress(100); msg.empty(); bar.empty(); conn.close(); return True, "Done"
    except Exception as e: conn.close(); return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor(); c.execute("PRAGMA table_info(bhxh)")
        all = [r[1] for r in c.fetchall()]
        return [c for c in all if not c.startswith('idx_') and c != 'master_search_idx' and 'kcb' not in c.lower() and c != 'index']
    except: return []
    finally: conn.close()

# --- 4. TÌM KIẾM ---
def search_data(mode, q):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'ai':
            key = clean_text(q); 
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{key}%',))
        elif mode == 'manual':
            conds, vals = [], []
            for c, v in q.items():
                if v and v.strip():
                    conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- 5. GIAO DIỆN ---
def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống</h2>", unsafe_allow_html=True)
    
    if not get_firestore_db():
        st.error("❌ Lỗi kết nối Database Đám Mây. Vui lòng kiểm tra Secrets.")
        return

    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                role = verify_login(u, p)
                if role:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': role})
                    log_action(u, "Login", "Đăng nhập thành công")
                    st.rerun()
                else: st.error("Sai thông tin đăng nhập")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Nhanh (AI)", "Chi tiết (Thủ công)"])
    with tab1:
        st.caption("Nhập tên, số thẻ, ngày sinh...")
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            # GHI NHẬT KÝ TÌM KIẾM AI
            log_action(st.session_state['username'], "Search AI", f"Từ khóa: {q}")
            
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df) == 1:
                    with st.expander("✨ AI Phân tích", expanded=True):
                        with st.spinner("AI đang đọc..."):
                            st.write(get_ai_response(f"Dữ liệu: {df.iloc[0].to_dict()}", "Chuyên gia BHXH tóm tắt quyền lợi."))
            else: st.warning("Không tìm thấy.")
    with tab2:
        defaults = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel_cols = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defaults)]
        if not sel_cols: sel_cols = cols[:4]
        with st.expander("⚙️ Cột tìm kiếm", expanded=True):
            selected = st.multiselect("Chọn cột:", cols, default=sel_cols)
        inputs = {}
        if selected:
            cols_per_row = 4
            for i in range(0, len(selected), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    if i + j < len(selected):
                        c_name = selected[i+j]
                        with row_cols[j]:
                            inputs[c_name] = st.text_input(f"Nhập {c_name}")
        if st.button("🔍 Tìm kiếm", type="primary"):
            valid = {k: v for k, v in inputs.items() if v.strip()}
            if valid:
                # GHI NHẬT KÝ TÌM KIẾM THỦ CÔNG
                log_action(st.session_state['username'], "Search Manual", str(valid))
                
                df = search_data('manual', valid)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không tìm thấy.")
            else: st.warning("Nhập ít nhất 1 ô.")

def render_chatbot():
    st.subheader("🤖 Trợ lý ảo BHXH")
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "model", "content": "Chào bạn! Tôi là trợ lý ảo BHXH/BHYT."}]
    for m in st.session_state.messages:
        with st.chat_message(m["role"]): st.markdown(m["content"])
    if prompt := st.chat_input("Hỏi gì đó..."):
        # GHI NHẬT KÝ CHAT
        log_action(st.session_state['username'], "Chatbot Query", prompt)
        
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("model"):
            message_placeholder = st.empty()
            full_response = ""
            stream_res = get_ai_response(prompt, "Bạn là chuyên gia tư vấn BHXH Việt Nam.", stream=True)
            try:
                if isinstance(stream_res, str):
                    full_response = stream_res; message_placeholder.markdown(full_response)
                else:
                    for chunk in stream_res:
                        if chunk.text: full_response += chunk.text; message_placeholder.markdown(full_response + "▌")
                    message_placeholder.markdown(full_response)
            except Exception as e: full_response = f"Lỗi: {str(e)}"; message_placeholder.markdown(full_response)
            st.session_state.messages.append({"role": "model", "content": full_response})

def render_content():
    st.subheader("✍️ Tạo Nội Dung")
    c1, c2 = st.columns(2)
    with c1:
        topic = st.text_input("Chủ đề:")
        if st.button("Viết bài", type="primary") and topic:
            # GHI NHẬT KÝ TẠO NỘI DUNG
            log_action(st.session_state['username'], "Content Creator", f"Chủ đề: {topic}")
            
            with st.spinner("Đang viết..."):
                st.session_state['content'] = get_ai_response(f"Viết bài tuyên truyền về: {topic}", "Chuyên viên truyền thông")
    with c2:
        if 'content' in st.session_state: st.text_area("Kết quả:", value=st.session_state['content'], height=400)

def render_admin():
    st.header("🛠️ Quản Trị (Cloud)")
    t1, t2 = st.tabs(["User", "Logs"])
    with t1:
        st.dataframe(get_all_users(), use_container_width=True)
        with st.form("add"):
            u = st.text_input("User"); p = st.text_input("Pass", type='password'); r = st.selectbox("Quyền", ["user", "admin"])
            if st.form_submit_button("Tạo User"):
                if create_user(u, p, r): 
                    st.success("Thành công!")
                    log_action(st.session_state['username'], "Admin: Add User", u)
                    time.sleep(1); st.rerun()
                else: st.error("Tên đã tồn tại")
        
        with st.expander("Xóa User"):
            u_del = st.text_input("Nhập username cần xóa:")
            if st.button("Xóa"):
                if u_del != "admin" and delete_user_cloud(u_del): 
                    st.success("Đã xóa")
                    log_action(st.session_state['username'], "Admin: Delete User", u_del)
                    time.sleep(1); st.rerun()
                else: st.error("Không thể xóa hoặc không tồn tại")

    with t2:
        st.write("Nhật ký hoạt động (Giờ Việt Nam):")
        if st.button("Tải lại Logs"): st.rerun()
        st.dataframe(get_logs(200), use_container_width=True)

def main():
    init_cloud_admin() # Tạo admin nếu chưa có trên Cloud
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    if not st.session_state['logged_in']: render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            if st.button("🔍 Tra cứu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🤖 Chatbot AI", use_container_width=True): st.session_state['page'] = 'chatbot'
            if st.button("✍️ Tạo nội dung", use_container_width=True): st.session_state['page'] = 'content'
            if st.session_state['role'] == 'admin':
                st.divider(); 
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False; st.rerun()
        
        cols = get_display_columns()
        p = st.session_state['page']
        if p == 'search': render_search(cols)
        elif p == 'chatbot': render_chatbot()
        elif p == 'content': render_content()
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    main()
