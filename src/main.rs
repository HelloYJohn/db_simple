use std::{env, io, mem};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

use crate::ExecuteResult::{ExecuteSuccess, ExecuteTableFull};
use crate::MetaCommandResult::{MetaCommandSuccess, MetaCommandUnrecognizedCommand};
use crate::PrepareResult::{PrepareNegativeId, PrepareStringTooLong, PrepareSuccess, PrepareSyntaxError, PrepareUnrecognizedStatement};
use crate::StatementType::{StatementInsert, StatementNone, StatementSelect};

///String -> [u8;_]
#[macro_export]
macro_rules! string_to_array {
    ($string:expr, $array_length:expr) => {{
        let mut array = [0u8; $array_length];

        for (&x, p) in $string.as_bytes().iter().zip(array.iter_mut()) {
            *p = x;
        }
        array
    }};
}

#[macro_export]
macro_rules! array_to_array {
    ($src:expr, $array_length:expr) => {{
        let mut array = [0u8; $array_length];

        for (&x, p) in $src.iter().zip(array.iter_mut()) {
            *p = x;
        }
        array
    }};
}

// 序列化结构体
pub unsafe fn serialize_struct<T: Sized>(src: &T) -> &[u8] {
    ::std::slice::from_raw_parts((src as *const T) as *const u8, ::std::mem::size_of::<T>())
}

// 反序列化结构体
pub unsafe fn deserialize_struct<T: Sized>(src: Vec<u8>) -> T {
    ::std::ptr::read(src.as_ptr() as *const _)
}

enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFail,
}

enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareNegativeId,
    PrepareStringTooLong,
    PrepareSyntaxError,
    PrepareUnrecognizedStatement
}

enum StatementType {
    StatementInsert,
    StatementSelect,
    StatementNone,
}
#[derive(Debug)]
struct InputBuffer {
    buffer : String,
}

struct Statement {
    kind: StatementType,
    row_to_insert: Row,
}

const COLUMN_USERNAME_SIZE:usize = 32;
const COLUMN_EMAIL_SIZE:usize = 255;

struct Row {
    id: usize,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    fn new(id: usize, username: String, email: String) ->Self {
        Row {
            id,
            username: string_to_array!(username, COLUMN_USERNAME_SIZE),
            email: string_to_array!(email, COLUMN_EMAIL_SIZE),
        }
    }
}
const ID_SIZE:usize = 4;
const USERNAME_SIZE:usize = 32;
const EMAIL_SIZE:usize = 255;
// const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROW_SIZE:usize = mem::size_of::<Row>();
const PAGE_SIZE:usize = 4096;
const TABLE_MAX_PAGES:usize = 100;
const ROWS_PER_PAGE:usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS:usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// struct Page {
//     buffer : [u8; PAGE_SIZE],
// }
#[derive(Clone, Copy)]
struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self ([0u8; PAGE_SIZE])
    }
}

struct Pager {
    file : File,
    file_length : usize,
    pages : [Option<Page>; TABLE_MAX_PAGES],
}

impl Pager {
    fn pager_open(filename : &str) -> Pager {
        let path = Path::new(filename);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let file_lenth = metadata.len() as usize;
        Pager {
            file: file,
            file_length: file_lenth,
            pages: [None; TABLE_MAX_PAGES],
        }
    }

    fn get_page(&mut self, page_num :usize) -> Option<Page> {
        if page_num >= TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
            exit(-1);
        }
        if self.pages[page_num].is_none() {
            let mut page = Page::new();
            // partial page at the end of the file
            let num_pages = (self.file_length + PAGE_SIZE - 1) / PAGE_SIZE;

            if page_num <= num_pages {
                let res = self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));

                if res.is_err() {
                    println!("Error seeking file {:?}", res);
                }

                let res = self.file.read(&mut page.0[..]);
                if res.is_err() {
                    println!("Error reading file {:?}", res);
                }
            }

            self.pages[page_num] = Some(page);
        }
        self.pages[page_num]
    }

    fn pager_flush(&mut self, page_num: usize, size: usize) {
        let page = self.pages[page_num];
        if page.is_none() {
            println!("Tried to flush null page");
            exit(-1);
        }

        let offset = self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));
        if offset.is_err() {
            println!("Error seeking file {:?}", offset);
            exit(-1);
        }

        let mut buf = vec![];
        for i in 0..size {
            buf.push(page.unwrap().0[i]);
        }

        let bytes_written = self.file.write(buf.as_slice());
        let flush = self.file.flush();
        if bytes_written.is_err() || flush.is_err() {
            println!("Error writing: {:?}, {:?}", bytes_written, flush);
        }
    }
}

struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn db_open(filename : &str) -> Self {
        let pager = Pager::pager_open(filename);
        let num_rows = (pager.file_length / ROW_SIZE) as usize;
        Self {
            num_rows : num_rows,
            pager: pager,
        }
    }

    fn db_close(&mut self) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            let page = self.pager.pages[i];
            if page.is_none() {
                continue;
            }
            self.pager.pager_flush(i, PAGE_SIZE);
        }

        let num_addition_rows = self.num_rows % ROWS_PER_PAGE;
        if num_addition_rows > 0 {
            let page_num = num_full_pages;
            if self.pager.pages[page_num].is_some() {
                self.pager.pager_flush(page_num, num_addition_rows * ROW_SIZE);
            }
        }
    }

    fn execute_insert(&mut self, statement : &mut Statement) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteTableFull;
        }

        let row = &statement.row_to_insert;
        let mut cursor = Cursor::table_end(self);

        let info = unsafe { serialize_struct(row) };

        let (page_index, byte_offset) = cursor.cursor_value();
        if let Some(Some(page)) = self.pager.pages.get_mut(page_index) {
            //参考: https://stackoverflow.com/questions/45081768/efficiently-copy-non-overlapping-slices-of-the-same-vector-in-rust?noredirect=1&lq=1
            page.0[byte_offset..(byte_offset + ROW_SIZE)].clone_from_slice(&info[..])
        }
        self.num_rows += 1;

        return ExecuteSuccess;
    }

    fn execute_select(&mut self) -> ExecuteResult {
        let mut cursor = Cursor::table_start(self);
        while !cursor.end_of_table {
            let (page_offset, bytes_offset) = cursor.cursor_value();
            let row = cursor.table.pager.pages[page_offset].unwrap().0[bytes_offset..(bytes_offset + ROW_SIZE)].to_vec();
            // let row = self.pages[page_offset].unwrap().map(|x| x.0[bytes_offset..(bytes_offset + ROW_SIZE)].to_vec());
            let row: Row = unsafe { deserialize_struct(row) };
            print_row(row);
            cursor.cursor_advance();
        }

        return ExecuteSuccess;
    }
}

struct Cursor<'a> {
    table : &'a mut Table,
    row_num : usize,
    end_of_table : bool,
}

impl <'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> Cursor {
        let end_of_table = table.num_rows == 0;
        Cursor {
            table,
            row_num: 0,
            end_of_table: end_of_table,
        }
    }

    fn table_end(table: &'a mut Table) -> Cursor {
        let row_num = table.num_rows;
        Cursor {
            table,
            row_num: row_num,
            end_of_table: true,
        }
    }

    fn cursor_value(&mut self) -> (usize, usize) {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        self.table.pager.get_page(page_num);

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        (page_num, byte_offset)
    }

    fn cursor_advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input() -> InputBuffer {
    let mut buffer = String::new();
    io::stdin().read_line(& mut buffer).expect("Failed to readline");
    buffer = buffer.trim().to_string();
    return InputBuffer {
        buffer : buffer,
    };
}

fn print_row(row : Row) {
    let trim_elems: [char; 1] = ['\0'];
    let username = String::from_utf8(row.username.to_vec()).expect("Error");
    let username = username.trim_end_matches(&trim_elems);
    let email = String::from_utf8(row.email.to_vec()).expect("Error");
    let email = email.trim_end_matches(&trim_elems);

    println!(
        "{} {:?} {:?}",
        row.id,
        username,
        email
    );
}

fn do_meta_command(input_buffer : &InputBuffer, table: &mut Table) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        table.db_close();
        exit(0);
    } else {
        return MetaCommandUnrecognizedCommand;
    }
}

impl Statement {
    fn new() -> Self {
        Self {
            kind: StatementNone,
            row_to_insert : Row {
                id: 0,
                username: [0u8; COLUMN_USERNAME_SIZE],
                email: [0u8; COLUMN_EMAIL_SIZE],
            }
        }
    }

    fn prepare_insert(&mut self, input_buffer: &InputBuffer) -> PrepareResult {
        self.kind = StatementInsert;
        let row =
            sscanf::sscanf!(input_buffer.buffer, "insert {usize} {str} {str}");
        if row.is_ok() {
            let (id,username, email) = row.unwrap();

            if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
                return PrepareStringTooLong;
            }
            self.row_to_insert.id = id;
            self.row_to_insert.username = string_to_array!(username, COLUMN_USERNAME_SIZE);
            self.row_to_insert.email = string_to_array!(email, COLUMN_EMAIL_SIZE);
        } else {
            return PrepareSyntaxError;
        }

        return PrepareSuccess;
    }

    fn prepare_statement(&mut self, input_buffer : &InputBuffer) -> PrepareResult{
        if input_buffer.buffer.starts_with("insert") {
            return self.prepare_insert(input_buffer);
        } else if input_buffer.buffer.starts_with("select") {
            self.kind = StatementSelect;
        } else {
            return PrepareUnrecognizedStatement;
        }

        return PrepareSuccess;
    }

    fn execute_statement(&mut self, table : &mut Table) -> ExecuteResult {
        match self.kind {
            StatementInsert => {
                table.execute_insert(self);
            }
            StatementSelect => {
                table.execute_select();
            }

            _ => {}
        }
        return ExecuteSuccess;
    }
}

fn main() {
    let mut args = env::args();
    assert!(args.len() > 1);
    let mut table = Table::db_open(&args.nth(1).unwrap());
    loop {
        print_prompt();
        let input_buffer = read_input();
        if input_buffer.buffer.starts_with(".") {
            match do_meta_command(&input_buffer, &mut table) {
                MetaCommandSuccess => {continue;}
                MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command {:?}", input_buffer.buffer);
                    continue;
                }
            }
        }
        let mut statement = Statement { kind: StatementType::StatementInsert, row_to_insert: Row {
            id: 0,
            username: [0u8; COLUMN_USERNAME_SIZE],
            email: [0u8; COLUMN_EMAIL_SIZE],
        } };
        let buffer = input_buffer.buffer.clone();
        match statement.prepare_statement(&input_buffer) {
            PrepareSuccess => {
            }
            PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of '{}'.", buffer);
                continue;
            }
            PrepareSyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
            PrepareNegativeId => {
                println!("ID must be positive.");
                continue;
            }
            PrepareStringTooLong=> {
                println!("String is too long.");
                continue;
            }
        }

        statement.execute_statement(&mut table);
        println!("Executed.");
    }
}
