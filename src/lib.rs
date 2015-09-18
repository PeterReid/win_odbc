extern crate libc;
extern crate kernel32;
extern crate checked_int_cast;
extern crate winapi;
#[macro_use] extern crate lazy_static;

use std::iter;
use std::ptr;
use std::mem::transmute;
use winapi::minwindef::{HMODULE, FARPROC};
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use checked_int_cast::CheckedIntCast;
use std::cmp::min;

extern "C" {
    pub fn Call_SQLAllocHandle(x: *mut libc::c_void) -> libc::c_int;
}

pub type SQLSMALLINT = libc::c_short;
pub type SQLUSMALLINT = libc::c_ushort;
pub type SQLRETURN = SQLSMALLINT;
pub type SQLHANDLE = *mut libc::c_void;
pub type SQLPOINTER = *mut libc::c_void;
pub type SQLHENV = SQLHANDLE;
pub type SQLHDBC = SQLHANDLE;
pub type SQLHSTMT = SQLHANDLE;
pub type SQLHWND = winapi::HWND;
pub type SQLINTEGER = i64;
pub type SQLWCHAR = winapi::winnt::WCHAR;
pub type SQLLEN = SQLINTEGER;

const SQL_HANDLE_ENV: SQLSMALLINT = 1;
const SQL_HANDLE_DBC: SQLSMALLINT = 2;
const SQL_HANDLE_STMT: SQLSMALLINT = 3;

const SQL_NO_DATA: SQLRETURN = 100;

pub const SQL_ATTR_ODBC_VERSION: SQLINTEGER = 200;
pub const SQL_OV_ODBC3: SQLINTEGER = 3;

#[allow(non_snake_case)]
struct OdbcFns {
    SQLAllocHandle: extern "stdcall" fn(
        HandleType: SQLSMALLINT,
        InputHandle: SQLHANDLE,
        OutputHandle: *mut SQLHANDLE
    ) -> SQLRETURN,
    
    SQLSetEnvAttr: extern "stdcall" fn(
        EnvironmentHandle: SQLHENV,
        Attribute: SQLINTEGER,
        Value: SQLPOINTER,
        StringLength: SQLINTEGER,
    ) -> SQLRETURN,
    
    SQLDriverConnectW: extern "stdcall" fn(
        hdbc: SQLHDBC,
        hwnd: SQLHWND,
        szConnStrIn: *const SQLWCHAR,
        cchConnStrIn: SQLSMALLINT,
        szConnStrOut: *mut SQLWCHAR,
        cchConnStrOutMax: SQLSMALLINT,
        pcchConnStrOut: *mut SQLSMALLINT,
        fDriverCompletion: SQLUSMALLINT,
    ) -> SQLRETURN,
    
    SQLExecDirectW: extern "stdcall" fn(
        hstmt: SQLHSTMT,
        szSqlStr: *const SQLWCHAR,
        TextLength: SQLINTEGER,
    ) -> SQLRETURN,
    
    SQLNumResultCols: extern "stdcall" fn(
        StatementHandle: SQLHSTMT,
        ColumnCount: *mut SQLSMALLINT,
    ) -> SQLRETURN,
    
    SQLFreeHandle: extern "stdcall" fn(
        HandleType: SQLSMALLINT,
        Handle: SQLHANDLE,
    ) -> SQLRETURN,
    
    SQLDisconnect: extern "stdcall" fn(
        ConnectionHandle: SQLHANDLE,
    ) -> SQLRETURN,
    
    SQLGetDiagRecW: extern "stdcall" fn(
        fHandleType: SQLSMALLINT,
        handle: SQLHANDLE,
        iRecord: SQLSMALLINT,
        szSqlState: *mut SQLWCHAR,
        pfNativeError: *mut SQLINTEGER,
        szErrorMsg: *mut SQLWCHAR,
        cchErrorMsgMax: SQLSMALLINT,
        pcchErrorMsg: *mut SQLSMALLINT,
    ) -> SQLRETURN,

    SQLBindCol: extern "stdcall" fn(
        StatementHandle: SQLHSTMT,
        ColumnNumber: SQLUSMALLINT,
        TargetType: SQLSMALLINT,
        TargetValue: SQLPOINTER,
        BufferLength: SQLLEN,
        StrLen_or_Ind: *mut SQLLEN,
    ) -> SQLRETURN,
    
    SQLFetch: extern "stdcall" fn(
        StatementHandle: SQLHSTMT,
    ) -> SQLRETURN,
    
    SQLFreeStmt: extern "stdcall" fn(
        StatementHandle: SQLHSTMT,
        OptioN: SQLUSMALLINT,
    ) -> SQLRETURN,
}

lazy_static! {
    static ref ODBC_FNS: OdbcFns = {
        OdbcFns::new()
    };
}

pub struct Env {
    env: SQLHENV
}

impl Env {
    pub fn new() -> OdbcResult<Env> {
        let mut env = ptr::null_mut();
        let ret = (ODBC_FNS.SQLAllocHandle)(SQL_HANDLE_ENV, ptr::null_mut(), &mut env);
        
        if ret>=0 {
            Ok(Env{env: env})
        } else {
            Err(Error{messages: vec!["Failed to allocate environment".to_owned()]})
        }
    }
    
    pub fn set_int_attr(&self, attribute: SQLINTEGER, value: SQLINTEGER) -> OdbcResult<()> {
        let value: SQLPOINTER = unsafe { transmute(value as isize) };
        sql_result((), (ODBC_FNS.SQLSetEnvAttr)(self.env, attribute, value, 0), SQL_HANDLE_ENV, self.env)
    }
}


impl Drop for Env {
    fn drop(&mut self) {
        if self.env != ptr::null_mut() {
            (ODBC_FNS.SQLFreeHandle)(SQL_HANDLE_ENV, self.env);
        }
    }
}

pub struct Connection {
    dbc: SQLHDBC
}

pub fn to_u16s<S: AsRef<OsStr> + ?Sized>(s: &S) -> Vec<u16> {
    let s: &OsStr = OsStr::new(s);
    let mut u16s: Vec<u16> = s.encode_wide().collect();
    u16s.push(0);
    u16s
}

impl Connection {
    pub fn new(env: &Env) -> OdbcResult<Connection> {
        let mut dbc = ptr::null_mut();
        let ret = (ODBC_FNS.SQLAllocHandle)(SQL_HANDLE_DBC, env.env, &mut dbc);
        
        sql_result(Connection{dbc: dbc}, ret, SQL_HANDLE_ENV, env.env)
    }
    
    pub fn connect(&self, odbc_string: &str) -> OdbcResult<()> {
        let str = to_u16s(odbc_string);
        
        sql_result((), (ODBC_FNS.SQLDriverConnectW)(
            self.dbc,
            ptr::null_mut(),
            str.as_ptr(),
            -3,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0 //SQL_DRIVER_NOPROMPT
        ), SQL_HANDLE_DBC, self.dbc)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.dbc != ptr::null_mut() {
            (ODBC_FNS.SQLDisconnect)(self.dbc);
            (ODBC_FNS.SQLFreeHandle)(SQL_HANDLE_DBC, self.dbc);
        }
    }
}



pub struct Statement {
    stmt: SQLHSTMT
}
impl Statement {
    pub fn new(connection: &Connection) -> OdbcResult<Statement> {
        let mut stmt = ptr::null_mut();
        let ret = (ODBC_FNS.SQLAllocHandle)(SQL_HANDLE_STMT, connection.dbc, &mut stmt);
        
        sql_result(Statement{stmt: stmt}, ret, SQL_HANDLE_DBC, connection.dbc)
    }
    
    pub fn exec_direct(&mut self, sql: &str) -> OdbcResult<()> {
        let wchars = to_u16s(sql);
        
        sql_result((), (ODBC_FNS.SQLExecDirectW)(self.stmt, wchars.as_ptr(), -3), SQL_HANDLE_STMT, self.stmt)
        
    }
    
    unsafe fn fetch(&mut self) -> OdbcResult<bool> {
        let res = (ODBC_FNS.SQLFetch)(self.stmt);
        if res == SQL_NO_DATA {
            return Ok(false)
        }
        sql_result(true, res, SQL_HANDLE_STMT, self.stmt)
    }
    
    pub fn run_string_select<F>(&mut self, sql: &str, mut f: F) -> OdbcResult<()>
        where F: FnMut(Vec<String>)
    {
        try!(self.exec_direct(sql));
        let result_col_count = try!(self.num_result_cols());
        
        let mut buffers: Vec<Vec<u16>> = (0..result_col_count).map(|_| {
            iter::repeat(0).take(1024).collect()
        }).collect();
        
        let mut buffer_lens: Vec<SQLLEN> = iter::repeat(255).take(buffers.len()*2).collect();
        for (idx, buffer) in buffers.iter_mut().enumerate() {
            if (ODBC_FNS.SQLBindCol)(self.stmt,
                (idx+1) as SQLUSMALLINT,
                -8,
                buffer.as_mut_ptr() as SQLPOINTER,
                buffer.len() as i64, // should the be *2 ??
                unsafe { buffer_lens.as_mut_ptr().offset(idx as isize) }//idx as isize) }
            ) != 0 {
                return Err(Error{messages: vec!["BindCol failed".to_owned()]});
            }
        }
        
        loop {
            if try!(unsafe { self.fetch() }) == false {
                break;
            }
            
            let result: Vec<String> = buffers.iter().zip(buffer_lens.iter()).map(|(buffer, buffer_len)| {
                let truncated_len = min(((*buffer_len)/2) as usize, buffer.len());
                let column_value = OsString::from_wide(&buffer[0..truncated_len]);
                column_value.into_string().ok().unwrap_or(String::new())
            }).collect();
            
            f(result);
        }
        
        (ODBC_FNS.SQLFreeStmt)(self.stmt, 2 /*SQL_UNBIND*/);
        
        Ok( () )
    }
    
    pub fn run_binary_select<F>(&mut self, sql: &str, mut f: F) -> OdbcResult<()>
        where F: FnMut(Vec<&[u8]>)
    {
        try!(self.exec_direct(sql));
        let result_col_count = try!(self.num_result_cols());
        
        let mut buffers: Vec<Vec<u8>> = (0..result_col_count).map(|_| {
            iter::repeat(0).take(4096).collect()
        }).collect();
        
        let mut buffer_lens: Vec<SQLLEN> = iter::repeat(255).take(buffers.len()*2).collect();
        for (idx, buffer) in buffers.iter_mut().enumerate() {
            if (ODBC_FNS.SQLBindCol)(self.stmt,
                (idx+1) as SQLUSMALLINT,
                -2, // SQL_C_BINARY
                buffer.as_mut_ptr() as SQLPOINTER,
                buffer.len() as i64,
                unsafe { buffer_lens.as_mut_ptr().offset(idx as isize) }//idx as isize) }
            ) != 0 {
                return Err(Error{messages: vec!["BindCol failed".to_owned()]});
            }
        }
        
        loop {
            if try!(unsafe { self.fetch() }) == false {
                break;
            }
            
            let result: Vec<&[u8]> = buffers.iter().zip(buffer_lens.iter()).map(|(buffer, buffer_len)| {
                let truncated_len = min(*buffer_len as usize, buffer.len());
                &buffer[0..truncated_len]
            }).collect();
            
            f(result);
        }
        
        (ODBC_FNS.SQLFreeStmt)(self.stmt, 2 /*SQL_UNBIND*/);
        
        Ok( () )
    }
    
    
    fn num_result_cols(&self) -> OdbcResult<usize> {
        let mut result_cols: SQLSMALLINT = 0;
        let ret = (ODBC_FNS.SQLNumResultCols)(self.stmt, &mut result_cols);
    
        sql_result(result_cols.as_usize_checked().unwrap_or(0), ret, SQL_HANDLE_STMT, self.stmt)
    }
}
impl Drop for Statement {
    fn drop(&mut self) {
        if self.stmt != ptr::null_mut() {
            //(ODBC_FNS.SQLFreeStmt)(SQL_HANDLE_STMT, self.stmt);
            (ODBC_FNS.SQLFreeHandle)(SQL_HANDLE_STMT, self.stmt);
        }
    }
}



impl OdbcFns {
    fn new() -> OdbcFns {
        let odbc32 = unsafe {
            kernel32::LoadLibraryA((& b"odbc32.dll\0"[..]).as_ptr() as *const i8)
        };
        
        fn make_fn(odbc32: HMODULE, name: &[u8]) -> FARPROC {
            let fn_ptr = unsafe {
                kernel32::GetProcAddress(odbc32, name.as_ptr() as *const i8)
            };
            fn_ptr
        }
        
        OdbcFns {
            SQLAllocHandle: unsafe { transmute(make_fn(odbc32, b"SQLAllocHandle\0")) },
            SQLSetEnvAttr: unsafe { transmute(make_fn(odbc32, b"SQLSetEnvAttr\0")) },
            SQLDriverConnectW: unsafe { transmute(make_fn(odbc32, b"SQLDriverConnectW\0")) },
            SQLExecDirectW: unsafe { transmute(make_fn(odbc32, b"SQLExecDirectW\0")) },
            SQLNumResultCols: unsafe { transmute(make_fn(odbc32, b"SQLNumResultCols\0")) },
            SQLFreeHandle: unsafe { transmute(make_fn(odbc32, b"SQLFreeHandle\0")) },
            SQLDisconnect: unsafe { transmute(make_fn(odbc32, b"SQLDisconnect\0")) },
            SQLGetDiagRecW: unsafe { transmute(make_fn(odbc32, b"SQLGetDiagRecW\0")) },
            SQLBindCol: unsafe { transmute(make_fn(odbc32, b"SQLBindCol\0")) },
            SQLFetch: unsafe { transmute(make_fn(odbc32, b"SQLFetch\0")) },
            SQLFreeStmt: unsafe { transmute(make_fn(odbc32, b"SQLFreeStmt\0")) },
            
        
        }
    }
}

#[derive(Debug)]
pub struct Error {
    messages: Vec<String>
}

pub type OdbcResult<V> = Result<V, Error>;

fn sql_result<V>(value: V, err: SQLRETURN, handle_type: SQLSMALLINT, handle: SQLHANDLE) -> OdbcResult<V> {
    if err >= 0 {
        Ok(value)
    } else {
        let mut error_strings: Vec<String> = Vec::new();
        if err == -2 {
            error_strings.push("Invalid handle".to_owned());
        } else {
            let mut msg : Vec<u16> = iter::repeat(0).take(1024).collect();
            let mut state = [0u16; 6];
            let mut error: SQLINTEGER = 0;
            
            let mut record_index: SQLSMALLINT = 1;
            let mut error_len: SQLSMALLINT = 0;
            while (ODBC_FNS.SQLGetDiagRecW)(
                handle_type,
                handle,
                record_index,
                (&mut state[..]).as_mut_ptr(),
                &mut error,
                msg.as_mut_ptr(),
                1024,
                &mut error_len) == 0
            {
                record_index+=1;
                /*// Hide data truncated..
                if (wcsncmp(wszState, L"01004", 5))
                {
                    fwprintf(stderr, L"[%5.5s] %s (%d)\n", wszState, wszMessage, iError);
                }*/
                
                
                let err = OsString::from_wide(&msg[0..error_len as usize]);
                error_strings.push(err.into_string().ok().unwrap_or("Invalid error string".to_owned()))
            }
        }
        
        println!("{:?}", error_strings);
        Err(Error{ messages: error_strings} )
    }
}

#[cfg(test)]
mod test{
    use super::*;
    use std::fs::File;
    use std::io::Write;
    
    fn go() -> OdbcResult<()> {
        let env = try!(Env::new());
        try!(env.set_int_attr(SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3));
        
        let conn = try!(Connection::new(&env));
        
        try!(conn.connect("Driver={SQL Server Native Client 11.0};Server=PL15\\SQLEXPRESS2012;Database=Reservation;Trusted_Connection=yes;MARS_Connection=yes"));
        
        let mut stmt = try!(Statement::new(&conn));
        
        let mut f = File::create("foo.sqlite3").unwrap();
        try!(stmt.run_binary_select("select data from page inner join page_usage on page.id = page_usage.page_id and page_usage.snapshot_id = (SELECT TOP 1 id FROM snapshot ORDER BY time DESC) ORDER BY page_usage.page_index", |x| {
            println!("{:?}", x);
            f.write_all(x[0]).unwrap();
        }));
        
        Ok( () )
    }

    #[test]
    fn simple() {
        match go() {
            Err(e) => {
                println!("Failed: {:?}", e)
            }
            Ok( () ) => {}
        }
        panic!("See output");
    }
}
