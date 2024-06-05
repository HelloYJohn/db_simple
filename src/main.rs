use std::{env, io, mem, process};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

use crate::ExecuteResult::{ExecuteDuplicateKey, ExecuteSuccess, ExecuteTableFull};
use crate::MetaCommandResult::{MetaCommandSuccess, MetaCommandUnrecognizedCommand};
use crate::NodeType::NodeLeaf;
use crate::PrepareResult::{PrepareNegativeId, PrepareStringTooLong, PrepareSuccess, PrepareSyntaxError, PrepareUnrecognizedStatement};
use crate::StatementType::{StatementInsert, StatementNone, StatementSelect};

///String -> [u8;_]
#[macro_export]
macro_rules! string_to_array {
    ($string:expr, $array_length: expr) => {{
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
    ExecuteDuplicateKey,
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

enum NodeType {
    NodeInternal,
    NodeLeaf,
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
const ID_SIZE:usize = std::mem::size_of::<usize>();
const USERNAME_SIZE:usize = 32;
const EMAIL_SIZE:usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
// const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROW_SIZE:usize = mem::size_of::<Row>();
const PAGE_SIZE:usize = 4096;
const TABLE_MAX_PAGES:usize = 100;

const NODE_TYPE_SIZE:usize = mem::size_of::<NodeType>();
const NODE_TYPE_OFFSET:usize = 0;
const IS_ROOT_SIZE:usize = mem::size_of::<bool>();
const IS_ROOT_OFFSET:usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE:usize = mem::size_of::<usize>();
const PARENT_POINTER_OFFSET:usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE:usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
*/
const LEAF_NODE_NUM_CELLS_SIZE:usize = mem::size_of::<usize>();
const LEAF_NODE_NUM_CELLS_OFFSET:usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE:usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE:usize = mem::size_of::<usize>();
const LEAF_NODE_KEY_OFFSET:usize = 0;
const LEAF_NODE_VALUE_SIZE:usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET:usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE:usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS:usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS:usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#[derive(Clone, Copy)]
struct Page([u8; PAGE_SIZE]);

impl Page {
    fn new() -> Self {
        Self ([0u8; PAGE_SIZE])
    }

    unsafe fn row_mut_slot(&mut self, cell_num: usize) -> Row {
        fn read_end_idx(bytes: &[u8]) -> usize {
            for i in (0..bytes.len()).rev() {
                if bytes[i] != 0 {
                    return i;
                }
            }
            0
        }
        let cell = self.leaf_node_value(cell_num);

        let id = std::ptr::read(cell as *const usize);
        let username_bytes = std::ptr::read((cell as usize + USERNAME_OFFSET) as *const [u8; USERNAME_SIZE]);
        let email_bytes = std::ptr::read((cell as usize + EMAIL_OFFSET) as *const [u8; EMAIL_SIZE]);

        Row {
            id,
            username: username_bytes,
            email: email_bytes,
        }
    }

    fn is_full(&self) -> bool {
        unsafe {
            self.leaf_node_num_cells() >= LEAF_NODE_MAX_CELLS
        }
    }

    fn index(&self, offset: usize) ->isize {
        let ptr = self.0.as_ptr();
        unsafe {
            (ptr as isize).checked_add(offset as isize).unwrap()
        }
    }

    unsafe fn leaf_node_mut_num_cells(&self) -> *mut usize {
        self.index(LEAF_NODE_NUM_CELLS_OFFSET) as *mut usize
    }

    fn set_leaf_node_num_cells(&mut self, num_cells: usize) {
        unsafe {
            *self.leaf_node_mut_num_cells() = num_cells
        }
    }

    fn leaf_node_num_cells(&self) -> usize {
        unsafe {*self.leaf_node_mut_num_cells()}
    }

    fn leaf_node_cell(&self, cell_num: usize) -> *const u8 {
        (self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE)) as *const u8
    }

    fn leaf_node_key(&self, cell_num: usize) -> usize {
        unsafe { *(self.leaf_node_cell(cell_num) as *mut usize) }
    }

    fn set_leaf_node_key(&self, cell_num: usize, key : usize){
        unsafe { *(self.leaf_node_cell(cell_num) as *mut usize) = key }
    }

    fn leaf_node_value(&self, cell_num: usize) -> *mut u8 {
        self.index(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE) as *mut u8
    }

    fn initialize_leaf_node(&mut self) {
        self.set_node_type(NodeLeaf);
        unsafe {*self.leaf_node_mut_num_cells() = 0;}
    }

    fn get_node_type<'a>(&self) -> &'a NodeType {
        unsafe {
            let ptr = self.index(NODE_TYPE_OFFSET) as *const NodeType;
            return &*ptr;
        }
    }

    fn set_node_type(&mut self, node_type: NodeType) {
        unsafe {
            let ptr = self.index(NODE_TYPE_OFFSET) as *mut NodeType;
            return *ptr = node_type;
        }
    }
}

struct Pager {
    file_descriptor: File,
    file_length : usize,
    num_pages : usize,
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

        let file_lenth = file.metadata().unwrap().len() as usize;
        let num_pages = file_lenth / PAGE_SIZE;

        if file_lenth % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(-1);
        }

        let mut pager = Pager {
            file_descriptor: file,
            file_length: file_lenth,
            num_pages: num_pages,
            pages: [None; TABLE_MAX_PAGES],
        };

        if pager.num_pages == 0 {
            unsafe {
                let root_node = pager.get_page(0);
                root_node.initialize_leaf_node();
            }
        }
        return pager;
    }

    fn get_page(&mut self, page_num : usize) -> &mut Page {
        if page_num >= TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
            exit(-1);
        }
        if self.pages[page_num].is_none() {
            let mut page = Page::new();
            // partial page at the end of the file
            let num_pages = (self.file_length + PAGE_SIZE - 1) / PAGE_SIZE;

            if page_num <= num_pages {
                let res = self.file_descriptor.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));

                if res.is_err() {
                    println!("Error seeking file {:?}", res);
                    exit(-1);
                }

                let res = self.file_descriptor.read(&mut page.0);
                if res.is_err() {
                    println!("Error reading file {:?}", res);
                    exit(-1);
                }
            }

            self.pages[page_num] = Some(page);

            if page_num >= self.num_pages {
                self.num_pages = page_num + 1;
            }
        }

        self.pages[page_num].as_mut().unwrap()
    }

    fn get_page_view(&self, page_num: usize) -> Page {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
        }

        self.pages[page_num].unwrap()
    }

    fn pager_flush(&mut self, page_num: usize) {
        let page = self.pages[page_num];
        if page.is_none() {
            println!("Tried to flush null page");
            exit(-1);
        }

        let offset = self.file_descriptor.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64));
        if offset.is_err() {
            println!("Error seeking file {:?}", offset);
            exit(-1);
        }

        let bytes_written = self.file_descriptor.write(&page.unwrap().0);
        let flush = self.file_descriptor.flush();
        if bytes_written.is_err() || flush.is_err() {
            println!("Error writing: {:?}, {:?}", bytes_written, flush);
            exit(-1);
        }
    }
}

struct Table {
    pager: Pager,
    root_page_num : usize,
}

impl Table {
    fn db_open(filename : &str) -> Self {
        let pager = Pager::pager_open(filename);

        Self {
            pager: pager,
            root_page_num: 0,
        }
    }

    fn db_close(&mut self) {
        let mut pager = &self.pager;

        for i in 0..pager.num_pages {
            let page = self.pager.pages[i];
            if page.is_none() {
                continue;
            }
            self.pager.pager_flush(i);
        }
    }

    fn execute_insert(&mut self, statement : &mut Statement) -> ExecuteResult {
        let row = &statement.row_to_insert;
        unsafe {
            let page = self.pager.get_page(self.root_page_num);
            if page.is_full() {
                return ExecuteTableFull;
            }
        }

        let (page_num, cell_num) = Cursor::table_find(self, row.id);
        let page = self.pager.get_page(page_num);

        if cell_num < page.leaf_node_num_cells() {
            let key_at_index = page.leaf_node_key(cell_num);
            if key_at_index == row.id {
                return ExecuteDuplicateKey;
            }
        }
        let mut cursor = Cursor {
            table : self,
            page_num,
            cell_num,
            end_of_table: false
        };
        unsafe {
            cursor.leaf_node_insert(row.id as usize, row);
        }

        return ExecuteSuccess;
    }

    fn execute_select(&mut self) -> ExecuteResult {
        let mut cursor = Cursor::table_start(self);
        while !cursor.end_of_table {
            let row = cursor.cursor_value();
            print_row(row);
            cursor.cursor_advance();
        }

        return ExecuteSuccess;
    }

    fn leaf_node_find(&mut self, key: usize) -> (usize, usize) {
        let page_num = self.root_page_num;
        let page = self.pager.get_page(page_num);
        let num_cells = page.leaf_node_num_cells();

        let mut min_index = 0;
        let mut one_past_max_index = num_cells;

        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;
            let key_at_index = page.leaf_node_key(index);
            if key == key_at_index {
                return (page_num, index);
            } else if key < key_at_index {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        return (page_num, min_index);
    }
}

struct Cursor<'a> {
    table : &'a mut Table,
    page_num : usize,
    cell_num : usize,
    end_of_table : bool,
}

impl <'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> Cursor {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num);
        let num_cells = root_node.leaf_node_num_cells();
        Cursor {
            table,
            page_num : root_page_num,
            cell_num : 0,
            end_of_table: num_cells == 0,
        }
    }

    fn table_find(table: &'a mut Table, key : usize) -> (usize, usize) {
        let root_node = table.pager.get_page(table.root_page_num);

        match root_node.get_node_type() {
            NodeType::NodeInternal => {
                println!("Need to implement searching an internal node");
                exit(-1);
            }
            NodeType::NodeLeaf => {
                return table.leaf_node_find(key);
            }
        }
    }

    fn cursor_value(&mut self) -> Row {
        let page = self.table.pager.get_page(self.page_num);
        let cell_num = self.cell_num;
        unsafe { page.row_mut_slot(cell_num) }
    }

    fn cursor_advance(&mut self) {
        let page = self.table.pager.get_page(self.page_num);
        self.cell_num += 1;
        unsafe {
            if self.cell_num >= page.leaf_node_num_cells() {
                self.end_of_table = true;
            }
        }
    }

    unsafe fn leaf_node_insert(&mut self, key: usize, value: &Row) {
        let cell_num = self.cell_num;
        let page = self.table.pager.get_page(self.page_num);
        let num_cells = page.leaf_node_num_cells();
        if num_cells > LEAF_NODE_MAX_CELLS {
            println!("Need to implement splitting a leaf node.");
            process::exit(-1);
        }
        if cell_num < num_cells {
            // shift cell from cell_num to num_cells to right to make room for new cell
            for i in (cell_num + 1..=num_cells).rev() {
                std::ptr::copy_nonoverlapping(page.leaf_node_cell(i - 1),
                                              page.leaf_node_cell(i) as *mut u8,
                                              LEAF_NODE_CELL_SIZE);
            }
        }
        page.set_leaf_node_num_cells(num_cells + 1);
        page.set_leaf_node_key(cell_num, key);

        let cell = page.leaf_node_value(cell_num);
        self.serialize_row(cell, value);
    }

    unsafe fn serialize_row(&self, cell: *mut u8, source: &Row) {
        std::ptr::write(cell as *mut usize, source.id as usize);

        std::ptr::write((cell as usize + USERNAME_OFFSET) as *mut [u8; USERNAME_SIZE], [0u8; USERNAME_SIZE]);
        std::ptr::copy(source.username.as_ptr(), (cell as usize + USERNAME_OFFSET) as *mut u8, source.username.len());

        std::ptr::write((cell as usize + EMAIL_OFFSET) as *mut [u8; EMAIL_SIZE], [0 as u8; EMAIL_SIZE]);
        std::ptr::copy(source.email.as_ptr(), (cell as usize + EMAIL_OFFSET) as *mut u8, source.email.len());
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

unsafe fn print_leaf_node(page : &mut Page) {
    let num_cells = page.leaf_node_num_cells();
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = page.leaf_node_key(i);
        println!("  - {} : {}", i, key)
    }
}
fn do_meta_command(input_buffer : &InputBuffer, table: &mut Table) -> MetaCommandResult {
    match input_buffer.buffer.as_str() {
        ".exit" => {
            table.db_close();
            exit(0);
        }

        ".btree" => unsafe {
            println!("Tree: ");
            print_leaf_node(table.pager.get_page(0));
            return MetaCommandSuccess;
        }
        _ => {
            return MetaCommandUnrecognizedCommand;
        }
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
                return table.execute_insert(self);
            }
            StatementSelect => {
                return table.execute_select();
            }

            _ => {
                return ExecuteSuccess;
            }
        }
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

        match statement.prepare_statement(&input_buffer) {
            PrepareSuccess => {
            }
            PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of '{}'.", &input_buffer.buffer);
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

        match statement.execute_statement(&mut table) {
            ExecuteSuccess => {
                println!("Executed.");
            }
            ExecuteTableFull => {
                println!("Error: Table full.");
            }
            ExecuteResult::ExecuteDuplicateKey => {
                println!("Error: Duplicate key.");
            }
        }

    }
}
