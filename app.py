# --- TỰ ĐỘNG NÂNG CẤP THƯ VIỆN AI NẾU CŨ (FIX LỖI 404) ---
import subprocess
import sys
try:
    import google.generativeai as genai
    import pkg_resources
    # Kiểm tra version, nếu thấp hơn 0.7.0 thì update ngay lập tức
    ver = pkg_resources.get_distribution("google-generativeai").version
    if ver < "0.7.0":
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
        import google.generativeai as genai
except:
    pass # Bỏ qua nếu lỗi import pkg_resources

import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
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
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
HARDCODED_API_KEY = "AIzaSyBd6MNZdWTsJiTy1yrrWK4G2PsltqFV6eg" 
ZALO_PHONE_NUMBER = "0986053006" 

# CÁC HẰNG SỐ TÍNH BHXH TỰ NGUYỆN (2024-2025)
CHUAN_NGHEO = 1500000 # Mức chuẩn nghèo khu vực nông thôn
LUONG_CO_SO = 2340000 # Mức lương cơ sở
MAX_MUC_DONG = 20 * LUONG_CO_SO # Mức đóng tối đa
TY_LE_DONG = 0.22 # Tỷ lệ đóng 22%
HO_TRO_NGHEO = 0.30 # Hỗ trợ 30% cho hộ nghèo
HO_TRO_CAN_NGHEO = 0.25 # Hỗ trợ 25% cho hộ cận nghèo
HO_TRO_KHAC = 0.10 # Hỗ trợ 10% cho đối tượng khác

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

# --- 1. KẾT NỐI DATABASE ĐÁM MÂY (FIREBASE) ---
@st.cache_resource
def get_firestore_db():
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return firestore.Client(credentials=creds, project=key_dict["project_id"])
    except: return None
    return None

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- QUẢN LÝ USER (CLOUD) ---
def create_user(username, password, role):
    db = get_firestore_db()
    if not db: return False
    doc_ref = db.collection("users").document(username)
    if doc_ref.get().exists: return False 
    doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
    return True

def verify_login(username, password):
    db = get_firestore_db()
    if not db: return None
    doc = db.collection("users").document(username).get()
    if doc.exists and doc.to_dict()["password"] == make_hashes(password): return doc.to_dict()["role"]
    return None

def delete_user_cloud(username):
    db = get_firestore_db()
    if db: db.collection("users").document(username).delete(); return True
    return False

def update_password(username, new_password):
    db = get_firestore_db()
    if db:
        try: db.collection("users").document(username).update({"password": make_hashes(new_password)}); return True
        except: return False
    return False

def get_all_users():
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try: return pd.DataFrame([{"username": d.id, **d.to_dict()} for d in db.collection("users").stream()])
    except: return pd.DataFrame()

# --- QUẢN LÝ LOGS (CLOUD) ---
def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            vn_timezone = datetime.timezone(datetime.timedelta(hours=7))
            now_vn = datetime.datetime.now(vn_timezone)
            db.collection("logs").add({
                "timestamp": now_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "sort_time": firestore.SERVER_TIMESTAMP,
                "username": username, "action": action, "details": str(details)
            })
    except: pass

def get_logs(limit=100):
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        return pd.DataFrame([{"Thời gian (VN)": d.to_dict().get("timestamp"), "Người dùng": d.to_dict().get("username"), "Hành động": d.to_dict().get("action"), "Chi tiết": d.to_dict().get("details")} for d in logs_ref.stream()])
    except: return pd.DataFrame()

def init_cloud_admin():
    if "admin_checked" not in st.session_state:
        if verify_login("admin", "admin123") is None: create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# --- 2. HỆ THỐNG AI (CƠ CHẾ MỚI) ---
def configure_ai():
    key = HARDCODED_API_KEY or st.session_state.get('user_api_key') or st.secrets.get("GOOGLE_API_KEY")
    if key: genai.configure(api_key=key); return True
    return False

def get_ai_response(prompt, role_desc="", stream=False):
    """Thử lần lượt các model từ mới đến cũ để tránh lỗi 404."""
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."
    models_to_try = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro"]
    full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
    last_error = ""
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            if stream: return model.generate_content(full_prompt, stream=True)
            return model.generate_content(full_prompt).text
        except Exception as e:
            last_error = str(e)
            if "429" in last_error: return "⚠️ Hệ thống đang quá tải. Vui lòng thử lại sau 1 phút."
            continue 
    return f"⚠️ Không kết nối được AI. Lỗi cuối cùng: {last_error}"

# --- 3. XỬ LÝ DỮ LIỆU ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db(); res = conn.execute("SELECT count(*) FROM bhxh").fetchone(); conn.close()
            if res and res[0] > 0: return True, "Sẵn sàng"
        except: os.remove(DB_FILE)
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty(); msg.info(f"📦 Đang nối {len(parts)} phần dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as o:
                for p in parts: with open(p, 'rb') as i: o.write(i.read())
            msg.info("📦 Đang giải nén..."); 
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty(); return True, "Restored"
        except Exception as e: return False, str(e)
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Thiếu dữ liệu"

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
            k = clean_text(q); 
            if not k: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{k}%',))
        else:
            conds, vals = [], []
            for c, v in q.items():
                if v.strip():
                    conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- TÍNH BHXH TỰ NGUYỆN ---
def format_vnd(value):
    return f"{int(value):,} VNĐ".replace(",", ".")

def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện")
    st.caption("Công cụ ước tính số tiền phải đóng dựa trên mức thu nhập bạn lựa chọn.")

    # 1. Nhập mức thu nhập
    st.markdown("#### 1. Chọn mức thu nhập làm căn cứ đóng")
    col_inp, col_info = st.columns([2, 1])
    
    with col_inp:
        # Thanh trượt chọn mức thu nhập (Bước nhảy 50k)
        income = st.slider(
            "Mức thu nhập (kéo thanh trượt):", 
            min_value=CHUAN_NGHEO, 
            max_value=MAX_MUC_DONG, 
            value=CHUAN_NGHEO,
            step=50000,
            format="%d"
        )
        st.info(f"Mức thu nhập bạn chọn: **{format_vnd(income)}**")
        
        # Nhập số chính xác nếu cần
        exact_income = st.number_input("Hoặc nhập số chính xác:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=income, step=1000)
        if exact_income != income:
            income = exact_income

    with col_info:
        st.markdown(
            f"""
            <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; font-size: 0.9em;">
            <b>Thông tin tham chiếu (2024):</b><br>
            - Tối thiểu: {format_vnd(CHUAN_NGHEO)}<br>
            - Tối đa: {format_vnd(MAX_MUC_DONG)}<br>
            - Tỷ lệ đóng: 22%
            </div>
            """, unsafe_allow_html=True
        )

    # 2. Chọn đối tượng
    st.markdown("#### 2. Chọn đối tượng ưu tiên (để tính mức hỗ trợ)")
    doi_tuong = st.radio(
        "Bạn thuộc đối tượng nào?",
        ["Khác (Hỗ trợ 10%)", "Hộ nghèo (Hỗ trợ 30%)", "Hộ cận nghèo (Hỗ trợ 25%)"],
        horizontal=True
    )

    # Tính toán
    # Mức đóng chuẩn (chưa trừ hỗ trợ) = Thu nhập * 22%
    muc_dong_chuan = income * TY_LE_DONG
    
    # Mức hỗ trợ của nhà nước = Chuẩn nghèo * % Hỗ trợ
    if "Hộ nghèo" in doi_tuong:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_NGHEO # 30% của 22% chuẩn nghèo
        tile_hotro = "30%"
    elif "Hộ cận nghèo" in doi_tuong:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_CAN_NGHEO # 25% của 22% chuẩn nghèo
        tile_hotro = "25%"
    else:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_KHAC # 10% của 22% chuẩn nghèo
        tile_hotro = "10%"

    # Số tiền thực đóng = Mức đóng chuẩn - Mức hỗ trợ
    so_tien_thuc_dong = muc_dong_chuan - muc_ho_tro

    # 3. Hiển thị kết quả (Bảng so sánh các phương thức đóng)
    st.markdown("---")
    st.markdown("#### 📊 Bảng Chi Tiết Số Tiền Phải Đóng")
    
    # Tạo dữ liệu cho bảng
    data = {
        "Phương thức": ["Hằng tháng", "3 tháng", "6 tháng", "12 tháng"],
        "Số tháng": [1, 3, 6, 12],
        "Tổng mức đóng (chưa giảm)": [],
        "Nhà nước hỗ trợ": [],
        "BẠN PHẢI ĐÓNG": []
    }

    for months in data["Số tháng"]:
        total_raw = muc_dong_chuan * months
        total_support = muc_ho_tro * months
        total_final = so_tien_thuc_dong * months
        
        data["Tổng mức đóng (chưa giảm)"].append(format_vnd(total_raw))
        data["Nhà nước hỗ trợ"].append(format_vnd(total_support))
        data["BẠN PHẢI ĐÓNG"].append(format_vnd(total_final))

    df_result = pd.DataFrame(data)
    
    # Highlight cột kết quả
    st.dataframe(
        df_result.style.highlight_max(axis=0, subset=["BẠN PHẢI ĐÓNG"], color='#e6ffe6'),
        use_container_width=True,
        hide_index=True
    )
    
    st.success(f"💡 **Kết luận:** Với mức thu nhập **{format_vnd(income)}**, đối tượng **{doi_tuong}**, bạn chỉ cần đóng **{format_vnd(so_tien_thuc_dong)}/tháng**.")


# --- 5. GIAO DIỆN ---
def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống</h2>", unsafe_allow_html=True)
    if not get_firestore_db(): st.error("❌ Lỗi kết nối Database Cloud."); return
    render_zalo_widget()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                r = verify_login(u, p)
                if r: st.session_state.update({'logged_in': True, 'username': u, 'role': r}); log_action(u, "Login", "Success"); st.rerun()
                else: st.error("Sai thông tin")

def render_change_password():
    st.subheader("🔒 Đổi Mật Khẩu")
    with st.form("change_pass"):
        o = st.text_input("Mật khẩu cũ", type="password")
        n = st.text_input("Mật khẩu mới", type="password")
        c = st.text_input("Nhập lại", type="password")
        if st.form_submit_button("Đổi"):
            u = st.session_state['username']
            if verify_login(u, o):
                if n == c and len(n) >= 6:
                    if update_password(u, n): st.success("Thành công!"); log_action(u, "ChangePass"); time.sleep(1); st.session_state['logged_in'] = False; st.rerun()
                    else: st.error("Lỗi mạng")
                else: st.warning("Mật khẩu không khớp/ngắn")
            else: st.error("Mật khẩu cũ sai")

def render_search(cols):
    st.subheader("🔍 Tra Cứu")
    t1, t2 = st.tabs(["Nhanh (AI)", "Chi tiết"])
    with t1:
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            log_action(st.session_state['username'], "Search AI", q)
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df) == 1:
                    with st.expander("✨ AI Phân tích"):
                        st.write(get_ai_response(f"Dữ liệu: {df.iloc[0].to_dict()}", "Chuyên gia BHXH tóm tắt."))
            else: st.warning("Không thấy.")
    with t2:
        defs = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defs)] or cols[:4]
        with st.expander("Cấu hình", expanded=True): s = st.multiselect("Cột:", cols, default=sel)
        inp = {}
        if s:
            c = st.columns(4)
            for i, n in enumerate(s): inp[n] = c[i % 4].text_input(n)
        if st.button("Tìm"):
            v = {k: val for k, val in inp.items() if val.strip()}
            if v:
                log_action(st.session_state['username'], "Search Manual", str(v))
                df = search_data('manual', v)
                if not df.empty:
                    st.success(f"Thấy {len(df)} KQ")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không thấy.")
            else: st.warning("Nhập thông tin.")

def render_chatbot():
    st.subheader("🤖 Chatbot")
    if "msg" not in st.session_state: st.session_state.msg = [{"role": "model", "content": "Chào bạn! Tôi có thể giúp gì?"}]
    for m in st.session_state.msg: st.chat_message(m["role"]).markdown(m["content"])
    if p := st.chat_input():
        log_action(st.session_state['username'], "Chatbot", p)
        st.session_state.msg.append({"role": "user", "content": p})
        st.chat_message("user").markdown(p)
        with st.chat_message("model"):
            ph = st.empty(); res = ""; 
            s = get_ai_response(p, "Chuyên gia BHXH Việt Nam.", True)
            try:
                if isinstance(s, str): ph.markdown(s); res = s
                else:
                    for c in s: 
                        if c.text: res += c.text; ph.markdown(res + "▌")
                    ph.markdown(res)
            except: ph.markdown(res)
            st.session_state.msg.append({"role": "model", "content": res})

def render_content():
    st.subheader("✍️ Tạo Nội Dung")
    c1, c2 = st.columns(2)
    with c1:
        t = st.text_input("Chủ đề:")
        if st.button("Viết") and t:
            log_action(st.session_state['username'], "Content", t)
            with st.spinner("..."): st.session_state['txt'] = get_ai_response(f"Viết về: {t}", "Chuyên viên truyền thông")
    with c2:
        if 'txt' in st.session_state: st.text_area("KQ:", value=st.session_state['txt'], height=400)

def render_admin():
    st.header("🛠️ Quản Trị")
    t1, t2 = st.tabs(["User", "Logs"])
    with t1:
        st.dataframe(get_all_users(), use_container_width=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            with st.popover("➕ Thêm"):
                with st.form("a"):
                    u = st.text_input("User"); p = st.text_input("Pass"); r = st.selectbox("Role", ["user", "admin"])
                    if st.form_submit_button("Tạo"):
                        if create_user(u, p, r): st.success("OK"); log_action(st.session_state['username'], "Add", u); time.sleep(1); st.rerun()
                        else: st.error("Trùng")
        with c2:
            with st.popover("🗑️ Xóa"):
                d = st.text_input("User xóa:")
                if st.button("Xóa"):
                    if d != "admin" and delete_user_cloud(d): st.success("OK"); log_action(st.session_state['username'], "Del", d); time.sleep(1); st.rerun()
                    else: st.error("Lỗi")
        with c3:
            with st.popover("🔄 Reset Pass"):
                rs = st.text_input("User reset (123456):")
                if st.button("Reset"):
                    if update_password(rs, "123456"): st.success("OK"); log_action(st.session_state['username'], "Reset", rs)
                    else: st.error("Lỗi")
    with t2:
        if st.button("Tải lại"): st.rerun()
        st.dataframe(get_logs(200), use_container_width=True)

def main():
    init_cloud_admin()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    render_zalo_widget()
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    if not st.session_state['logged_in']: render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            if st.button("🔍 Tra cứu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🧮 Tính BHXH", use_container_width=True): st.session_state['page'] = 'calc'
            if st.button("🤖 Chatbot", use_container_width=True): st.session_state['page'] = 'chat'
            if st.button("✍️ Nội dung", use_container_width=True): st.session_state['page'] = 'content'
            st.divider()
            if st.button("🔒 Đổi mật khẩu", use_container_width=True): st.session_state['page'] = 'pass'
            if st.session_state['role'] == 'admin':
                st.divider(); 
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout"); st.session_state['logged_in'] = False; st.rerun()
        
        p = st.session_state['page']; cols = get_display_columns()
        if p == 'search': render_search(cols)
        elif p == 'calc': render_calculator()
        elif p == 'chat': render_chatbot()
        elif p == 'content': render_content()
        elif p == 'pass': render_change_password()
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    main()
