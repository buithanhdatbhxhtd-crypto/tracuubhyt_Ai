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

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Big Data",
    page_icon="🗃️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB = 'users.db'

# --- 1. HỆ THỐNG USER & LOG ---
def init_user_db():
    """Khởi tạo DB User + Kích hoạt WAL Mode"""
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    
    # Tự động sửa bảng logs nếu thiếu cột
    try:
        c.execute("SELECT * FROM logs LIMIT 1")
        cols = [d[0] for d in c.description]
        if "details" not in cols: raise Exception()
    except:
        c.execute("DROP TABLE IF EXISTS logs")
        c.execute('''CREATE TABLE logs 
                     (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Tạo admin mặc định
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def log_action(username, action, details=""):
    try:
        conn = init_user_db()
        c = conn.cursor()
        # Ghi nhật ký với thời gian thực
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, str(details)))
        conn.commit()
        conn.close()
    except: pass

def configure_gemini():
    key = st.secrets.get("GOOGLE_API_KEY", st.session_state.get('user_api_key', ''))
    if key: 
        genai.configure(api_key=key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU ---
def clean_text(text):
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    # 1. Kiểm tra DB có sẵn
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db()
            res = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'").fetchone()
            conn.close()
            if res and res[0] > 0: return True, "Dữ liệu đã sẵn sàng"
        except:
            os.remove(DB_FILE) # DB lỗi thì xóa

    # 2. Kiểm tra các file nén bị cắt (bhxh_data.zip.001, .002...)
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Tìm thấy {len(parts)} phần dữ liệu phân mảnh. Đang ghép nối...")
        try:
            full_zip = "bhxh_data_full.zip"
            with open(full_zip, 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            
            msg.info("📦 Đang giải nén dữ liệu...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            
            if os.path.exists(full_zip): os.remove(full_zip)
            
            msg.success("✅ Đã khôi phục dữ liệu thành công!")
            time.sleep(1)
            msg.empty()
            return True, "Restored"
        except Exception as e:
            return False, f"Lỗi ghép file: {str(e)}"

    # 3. Fallback: File Excel gốc
    if os.path.exists(EXCEL_FILE):
        return import_excel_to_sqlite()

    return False, "⚠️ Không tìm thấy dữ liệu. Hãy upload các file bhxh_data.zip.001... lên GitHub"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp trực tiếp từ Excel (Chậm). Khuyên dùng tool 'local_converter.py' để tối ưu.")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info(f"⏳ Đang xử lý '{EXCEL_FILE}'...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(40)
        
        # Chuẩn hóa tên cột
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        
        # Chuyển đổi dữ liệu
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        
        # Tạo cột Master Search
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        # Tạo Index cho từng cột
        for col in df.columns:
            if col != 'master_search_idx':
                df[f'idx_{col}'] = df[col].apply(clean_text)
        
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        
        bar.progress(100); time.sleep(0.5); msg.empty(); bar.empty(); conn.close()
        return True, "Done"
    except Exception as e:
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    """Lấy danh sách cột hiển thị, loại bỏ các cột hệ thống và cột KCB"""
    conn = init_data_db()
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(bhxh)")
        all_cols = [r[1] for r in c.fetchall()]
        
        # Lọc bỏ cột index hệ thống và cột KCB nếu muốn
        clean_cols = []
        for col in all_cols:
            if col.startswith('idx_') or col == 'master_search_idx' or col == 'index':
                continue
            # Bỏ cột KCB nếu tìm thấy trong tên (tùy chọn)
            if 'kcb' in col.lower():
                continue
            clean_cols.append(col)
            
        return clean_cols
    except: return []
    finally: conn.close()

# --- 3. TÌM KIẾM ---
def search_data(mode, q):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    
    # Chọn các cột để hiển thị
    sel = ", ".join([f'"{c}"' for c in cols])
    
    try:
        if mode == 'ai':
            key = clean_text(q)
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{key}%',))
        
        elif mode == 'manual':
            conds, vals = [], []
            for c, v in q.items():
                if v and v.strip():
                    # Map tên cột hiển thị sang tên cột DB (lowercase, no space)
                    db_col = unidecode.unidecode(c).strip().replace(' ', '_').lower()
                    # Kiểm tra xem cột index có tồn tại không trước khi query
                    conds.append(f'idx_{db_col} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            
            if not conds: return pd.DataFrame()
            sql = f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50'
            return pd.read_sql_query(sql, conn, params=tuple(vals))
    except Exception as e:
        st.error(f"Lỗi tìm kiếm: {e}")
        return pd.DataFrame()
    finally: conn.close()

# --- 4. GIAO DIỆN ---
def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                conn = init_user_db()
                res = conn.execute('SELECT * FROM users WHERE username=? AND password=?', (u, make_hashes(p))).fetchone()
                conn.close()
                if res:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': res[2]})
                    log_action(u, "Login", "Thành công")
                    st.rerun()
                else: st.error("Sai tên đăng nhập hoặc mật khẩu")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Thông minh (AI)", "Chính xác (Thủ công)"])
    
    with tab1:
        st.info("Nhập bất kỳ thông tin nào bạn có: Tên viết liền, số thẻ, ngày sinh...")
        q = st.text_input("Từ khóa:", placeholder="Ví dụ: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df)==1 and configure_gemini():
                    with st.expander("✨ AI Phân tích hồ sơ"):
                        try:
                            model = genai.GenerativeModel('gemini-pro')
                            res = model.generate_content(f"Dữ liệu BHXH: {df.iloc[0].to_dict()}. Tóm tắt quyền lợi bảo hiểm.")
                            st.write(res.text)
                        except: st.warning("Lỗi kết nối AI")
            else: st.warning("Không tìm thấy kết quả nào.")

    with tab2:
        # --- PHẦN CẤU HÌNH MẶC ĐỊNH ---
        # Xác định các cột ưu tiên hiển thị mặc định
        priority_cols = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd', 'cccd']
        default_selection = []
        
        # Lọc ra các cột có thực trong file Excel khớp với ưu tiên
        for p_col in priority_cols:
            for col in cols:
                # So sánh tương đối (bỏ dấu, viết thường) để khớp tên cột
                if p_col in unidecode.unidecode(col).lower():
                    default_selection.append(col)
                    break # Lấy cột đầu tiên khớp
        
        # Nếu không tìm thấy cột ưu tiên nào thì lấy 3 cột đầu
        if not default_selection:
            default_selection = cols[:3]

        with st.expander("⚙️ Cấu hình bộ lọc", expanded=True):
            selected_cols = st.multiselect("Chọn cột cần tìm kiếm:", cols, default=default_selection)
        
        # --- SỬA LỖI BẬC THANG (Staircase Layout) ---
        inputs = {}
        if selected_cols:
            # Tạo các cột giao diện MỘT LẦN DUY NHẤT
            ui_cols = st.columns(len(selected_cols))
            
            # Duyệt qua từng cột đã chọn và đặt ô input vào đúng cột giao diện
            for i, col_name in enumerate(selected_cols):
                with ui_cols[i]:
                    inputs[col_name] = st.text_input(f"Nhập {col_name}", key=f"search_{col_name}")
        
        if st.button("🔍 Tìm kiếm ngay", type="primary"):
            # Lọc bỏ các ô trống
            valid_inputs = {k: v for k, v in inputs.items() if v.strip()}
            
            if valid_inputs:
                df = search_data('manual', valid_inputs)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    log_action(st.session_state['username'], "Search Manual", str(valid_inputs))
                else: st.warning("Không tìm thấy dữ liệu phù hợp.")
            else:
                st.warning("Vui lòng nhập ít nhất một thông tin để tìm kiếm.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    conn = init_user_db()
    
    if st.button("🧹 Xóa Cache & Reset Ứng Dụng"):
        st.cache_data.clear()
        st.rerun()

    t1, t2 = st.tabs(["👥 Quản lý Người dùng", "📜 Nhật ký Hoạt động"])
    
    with t1:
        st.subheader("Danh sách tài khoản")
        # Hiển thị bảng user
        users_df = pd.read_sql("SELECT username, role FROM users", conn)
        st.dataframe(users_df, use_container_width=True, hide_index=True)
        
        st.divider()
        
        # Chia 2 cột cho Thêm và Xóa
        c_add, c_del = st.columns(2)
        
        with c_add:
            st.subheader("Thêm User Mới")
            with st.form("add_user_form"):
                new_u = st.text_input("Username mới")
                new_p = st.text_input("Password", type="password")
                new_r = st.selectbox("Quyền hạn", ["user", "admin"])
                if st.form_submit_button("Tạo tài khoản"):
                    if new_u and new_p:
                        try:
                            conn.execute("INSERT INTO users VALUES (?,?,?)", (new_u, make_hashes(new_p), new_r))
                            conn.commit()
                            st.success(f"Đã tạo user: {new_u}")
                            log_action(st.session_state['username'], "Add User", new_u)
                            time.sleep(1); st.rerun()
                        except sqlite3.IntegrityError:
                            st.error("Tên đăng nhập đã tồn tại!")
                    else:
                        st.warning("Vui lòng nhập đủ thông tin.")

        with c_del:
            st.subheader("Xóa User")
            # Lọc danh sách user (trừ admin gốc)
            user_list = [u for u in users_df['username'] if u != 'admin']
            if user_list:
                user_to_del = st.selectbox("Chọn user cần xóa:", user_list)
                if st.button("🗑️ Xóa vĩnh viễn", type="primary"):
                    conn.execute("DELETE FROM users WHERE username=?", (user_to_del,))
                    conn.commit()
                    st.success(f"Đã xóa user: {user_to_del}")
                    log_action(st.session_state['username'], "Delete User", user_to_del)
                    time.sleep(1); st.rerun()
            else:
                st.info("Không có tài khoản phụ nào để xóa.")

    with t2:
        st.subheader("Lịch sử truy cập (Mới nhất trước)")
        if st.button("Xóa toàn bộ Nhật ký"):
            conn.execute("DELETE FROM logs")
            conn.commit()
            st.success("Đã dọn dẹp nhật ký!")
            st.rerun()
            
        # Hiển thị logs mới nhất lên đầu
        logs_df = pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 500", conn)
        st.dataframe(logs_df, use_container_width=True, hide_index=True)
        
    conn.close()

# --- MAIN ---
def main():
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    
    # Kiểm tra dữ liệu
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return

    if not st.session_state['logged_in']:
        render_login()
    else:
        # Sidebar Menu
        with st.sidebar:
            st.title(f"Xin chào, {st.session_state['username']}")
            
            if st.button("📋 Tra cứu Thủ công", use_container_width=True): st.session_state['page'] = 'manual'
            if st.button("🤖 Tra cứu AI", use_container_width=True): st.session_state['page'] = 'ai'
            
            if st.session_state['role'] == 'admin':
                st.divider()
                if st.button("🛠️ Quản trị hệ thống", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.divider()
            
            # Cấu hình API Key ẩn gọn
            with st.expander("Cấu hình AI Key"):
                k = st.text_input("Google API Key", type="password", value=st.session_state.get('user_api_key',''))
                if k: st.session_state['user_api_key'] = k

            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        # Routing Pages
        cols = get_display_columns()
        if st.session_state['page'] == 'search' or st.session_state['page'] == 'ai': 
            render_search(cols) # Default to search page logic but split inside function
        elif st.session_state['page'] == 'manual':
            render_search(cols) # Reuse render_search but tab 2 will be active if implemented that way, here simplified to render_search handling both tabs
        elif st.session_state['page'] == 'admin':
            render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
