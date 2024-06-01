use std::{io, mem};
use std::cmp::PartialEq;
use std::io::Write;
use crate::ExecuteResult::{ExecuteSuccess, ExecuteTableFull};
use crate::MetaCommandResult::{MetaCommandSuccess, MetaCommandUnrecognizedCommand};
use crate::PrepareResult::{PrepareSuccess, PrepareSyntaxError, PrepareUnrecognizedStatement};
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
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self ([0u8; PAGE_SIZE])
    }
}

struct Table {
    num_rows: usize,
    pages:[Option<Page>; TABLE_MAX_PAGES],
}

impl Table {
    fn new () -> Self {
        Self {
            num_rows : 0,
            pages: [None; TABLE_MAX_PAGES],
        }
    }

    fn row_slot(&mut self, row_num :usize) -> (usize, usize) {
        let page_num = row_num / ROWS_PER_PAGE;
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Option::from(Page::new())
        }
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        (page_num, byte_offset)
    }

    fn execute_insert(&mut self, statement : &mut Statement) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteTableFull;
        }

        let row = &statement.row_to_insert;

        let info = unsafe { serialize_struct(row) };

        let (page_index, byte_offset) = self.row_slot(self.num_rows);
        if let Some(Some(page)) = self.pages.get_mut(page_index) {
            //参考: https://stackoverflow.com/questions/45081768/efficiently-copy-non-overlapping-slices-of-the-same-vector-in-rust?noredirect=1&lq=1
            page.0[byte_offset..(byte_offset + ROW_SIZE)].clone_from_slice(&info[..])
        }
        self.num_rows += 1;

        return ExecuteSuccess;
    }

    fn execute_select(&mut self) -> ExecuteResult {
        for i in 0..self.num_rows {
            let (page_offset, bytes_offset) = self.row_slot(i);
            // let row = self.pages[page_offset].unwrap().0[bytes_offset..(bytes_offset + ROW_SIZE)].to_vec();
            let row = self.pages[page_offset].unwrap().0[bytes_offset..(bytes_offset + ROW_SIZE)].to_vec();
            // let row = self.pages[page_offset].unwrap().map(|x| x.0[bytes_offset..(bytes_offset + ROW_SIZE)].to_vec());
            let row: Row = unsafe { deserialize_struct(row) };

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

        return ExecuteSuccess;
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

fn print_row(row : &Row) {
    println!("({}, {:?}, {:?})", row.id, row.username, row.email);
}

fn do_meta_command(input_buffer : InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        std::process::exit(0);
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
    let mut table = Table::new();
    loop {
        print_prompt();
        let input_buffer = read_input();
        let buffer = input_buffer.buffer.clone();
        let first_char = &input_buffer.buffer[0..1];
        if first_char.eq(".") {
            match do_meta_command(input_buffer) {
                MetaCommandSuccess => {continue;}
                MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command {}", buffer);
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
            _ => {
                println!("other error");
                continue;
            }
        }

        statement.execute_statement(&mut table);
        println!("Executed.");
    }
}
